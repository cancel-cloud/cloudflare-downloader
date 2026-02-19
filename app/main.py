import atexit
import logging
import os
import shutil
import uuid
from urllib.parse import unquote

from flask import Flask, abort, g, jsonify, render_template, request, send_from_directory
from yt_dlp import YoutubeDL, version as yt_dlp_version

from .logging_config import configure_logging
from .metrics import MetricsRecorder, metrics_response
from .queue_manager import PRESET_CONFIG, QueueManager, build_runtime_diagnostics
from .repository import DownloadRepository

LOGGER = logging.getLogger(__name__)


def _normalize_external_url(raw: str) -> str | None:
    if not raw:
        return None
    url = unquote(raw)
    qs = request.query_string.decode("utf-8")
    if qs:
        url = url + ("&" if "?" in url else "?") + qs

    if url.startswith("http:/") and not url.startswith("http://"):
        url = url.replace("http:/", "http://", 1)
    if url.startswith("https:/") and not url.startswith("https://"):
        url = url.replace("https:/", "https://", 1)

    if url.startswith("http://") or url.startswith("https://"):
        return url
    return None


def _is_valid_url(url: str) -> bool:
    return url.startswith("http://") or url.startswith("https://")


def _parse_positive_int(value: str | None, default: int, minimum: int, maximum: int) -> int:
    try:
        parsed = int(value or str(default))
    except ValueError:
        return default
    return max(minimum, min(maximum, parsed))


def _safe_relative_path(base_dir: str, relative_path: str) -> str | None:
    normalized = relative_path.replace("\\", "/").lstrip("/")
    full = os.path.realpath(os.path.join(base_dir, normalized))
    base = os.path.realpath(base_dir)
    if os.path.commonpath([full, base]) != base:
        return None
    return os.path.relpath(full, base)


def create_app(config: dict | None = None) -> Flask:
    configure_logging(os.environ.get("LOG_LEVEL", "INFO"))

    app = Flask(__name__)
    base_download_dir = os.environ.get("BASE_DOWNLOAD_DIR") or "/tmp/cloudflare-downloader-data"
    sqlite_path = os.environ.get("SQLITE_PATH", os.path.join(base_download_dir, "downloader.db"))

    app.config.update(
        BASE_DOWNLOAD_DIR=base_download_dir,
        SQLITE_PATH=sqlite_path,
        MAX_CONCURRENT_DOWNLOADS=int(os.environ.get("MAX_CONCURRENT_DOWNLOADS", 4)),
        MIN_FREE_DISK_MB=int(os.environ.get("MIN_FREE_DISK_MB", 512)),
        JOB_PROGRESS_FLUSH_INTERVAL_MS=int(os.environ.get("JOB_PROGRESS_FLUSH_INTERVAL_MS", 750)),
        START_QUEUE_MANAGER=True,
    )

    if config:
        app.config.update(config)

    os.makedirs(app.config["BASE_DOWNLOAD_DIR"], exist_ok=True)

    repo = DownloadRepository(app.config["SQLITE_PATH"])
    repo.init()
    recovered = repo.recover_interrupted_downloads()

    metrics = MetricsRecorder()
    queue_manager = QueueManager(
        repo=repo,
        metrics=metrics,
        base_download_dir=app.config["BASE_DOWNLOAD_DIR"],
        max_concurrent_downloads=app.config["MAX_CONCURRENT_DOWNLOADS"],
        progress_flush_interval_ms=app.config["JOB_PROGRESS_FLUSH_INTERVAL_MS"],
    )

    app.extensions["repo"] = repo
    app.extensions["queue_manager"] = queue_manager
    app.extensions["metrics"] = metrics

    if app.config.get("START_QUEUE_MANAGER", True):
        queue_manager.start()

    diagnostics = build_runtime_diagnostics(
        base_download_dir=app.config["BASE_DOWNLOAD_DIR"],
        max_concurrent_downloads=app.config["MAX_CONCURRENT_DOWNLOADS"],
    )
    LOGGER.info(
        "startup",
        extra={
            "yt_dlp_version": yt_dlp_version.__version__,
            "recovered_jobs": recovered,
            **diagnostics,
        },
    )

    @atexit.register
    def _shutdown_queue_manager() -> None:
        queue_manager.stop()

    @app.before_request
    def _before_request() -> None:
        g.request_started_at = metrics.http_before_request()
        g.request_id = str(uuid.uuid4())

    @app.after_request
    def _after_request(response):
        started = getattr(g, "request_started_at", None)
        if started is not None:
            metrics.http_after_request(started, response.status_code)
        response.headers["X-Request-ID"] = getattr(g, "request_id", "")
        LOGGER.info(
            "http_request",
            extra={
                "request_id": getattr(g, "request_id", ""),
                "method": request.method,
                "path": request.path,
                "status": response.status_code,
                "remote_addr": request.remote_addr,
            },
        )
        return response

    def _enqueue_download(url: str, preset: str) -> dict:
        download_id = str(uuid.uuid4())
        record = repo.create_download(download_id, url, preset)
        metrics.mark_queued(preset)
        LOGGER.info("job_queued", extra={"job_id": download_id, "preset": preset, "url": url})
        return record

    @app.route("/", methods=["GET"])
    def index():
        url = (request.args.get("u") or "").strip()
        return render_template(
            "index.html",
            url=url,
            preset_options=PRESET_CONFIG,
            default_preset="best",
        )

    @app.route("/gallery", methods=["GET"])
    def gallery():
        page = _parse_positive_int(request.args.get("page"), 1, 1, 100000)
        per_page = _parse_positive_int(request.args.get("per_page"), 24, 1, 100)
        q = (request.args.get("q") or "").strip() or None
        sort = (request.args.get("sort") or "created_desc").strip()
        uploader = (request.args.get("uploader") or "").strip() or None
        status = (request.args.get("status") or "completed").strip() or None

        rows, total = repo.list_downloads(
            page=page,
            per_page=per_page,
            status=status,
            q=q,
            sort=sort,
            uploader=uploader,
        )

        videos = []
        for row in rows:
            media_path = row.get("media_local_path")
            videos.append(
                {
                    "id": row.get("id"),
                    "filename": os.path.basename(media_path) if media_path else None,
                    "media_local_path": media_path,
                    "title": row.get("title") or row.get("video_id") or row.get("requested_url"),
                    "uploader": row.get("uploader") or "Unknown",
                    "thumbnail": row.get("thumbnail_local_path"),
                    "original_url": row.get("webpage_url") or row.get("requested_url"),
                    "status": row.get("status"),
                    "created_at": row.get("created_at"),
                }
            )

        pages = (total + per_page - 1) // per_page if per_page else 1

        return render_template(
            "gallery.html",
            videos=videos,
            page=page,
            per_page=per_page,
            pages=pages,
            total=total,
            q=q or "",
            sort=sort,
            uploader=uploader or "",
            status=status or "",
        )

    @app.route("/files/<path:filename>")
    def serve_file(filename: str):
        safe_rel = _safe_relative_path(app.config["BASE_DOWNLOAD_DIR"], filename)
        if not safe_rel:
            abort(403)

        full = os.path.realpath(os.path.join(app.config["BASE_DOWNLOAD_DIR"], safe_rel))
        if not os.path.isfile(full):
            abort(404)

        return send_from_directory(app.config["BASE_DOWNLOAD_DIR"], safe_rel)

    @app.route("/download", methods=["POST"])
    def download_route():
        url = (request.form.get("u") or "").strip()
        preset = (request.form.get("preset") or "best").strip()

        if not _is_valid_url(url):
            return jsonify({"ok": False, "error": "Ungültige URL"}), 400
        if preset not in PRESET_CONFIG:
            return jsonify({"ok": False, "error": "invalid_preset"}), 400

        record = _enqueue_download(url, preset)
        return (
            jsonify(
                {
                    "ok": True,
                    "job_id": record.get("id"),
                    "preset": preset,
                    "status": record.get("status"),
                }
            ),
            202,
        )

    @app.route("/api/status/<job_id>", methods=["GET"])
    def get_job_status(job_id: str):
        job = repo.get_download(job_id)
        if not job:
            return jsonify({"ok": False, "error": "Job nicht gefunden"}), 404
        return jsonify({"ok": True, "job": job})

    @app.route("/api/jobs", methods=["GET"])
    def list_jobs():
        page = _parse_positive_int(request.args.get("page"), 1, 1, 100000)
        per_page = _parse_positive_int(request.args.get("per_page"), 20, 1, 100)
        status = (request.args.get("status") or "").strip() or None
        q = (request.args.get("q") or "").strip() or None
        sort = (request.args.get("sort") or "created_desc").strip()
        uploader = (request.args.get("uploader") or "").strip() or None

        items, total = repo.list_downloads(
            page=page,
            per_page=per_page,
            status=status,
            q=q,
            sort=sort,
            uploader=uploader,
        )

        pages = (total + per_page - 1) // per_page if per_page else 1
        return jsonify(
            {
                "ok": True,
                "items": items,
                "page": page,
                "per_page": per_page,
                "pages": pages,
                "total": total,
            }
        )

    @app.route("/api/jobs/<job_id>/pause", methods=["POST"])
    def pause_job(job_id: str):
        ok, state = queue_manager.pause_job(job_id)
        if not ok:
            if not repo.get_download(job_id):
                return jsonify({"ok": False, "error": "not_found"}), 404
            return jsonify({"ok": False, "error": state}), 409
        job = repo.get_download(job_id)
        return jsonify({"ok": True, "state": state, "job": job})

    @app.route("/api/jobs/<job_id>/resume", methods=["POST"])
    def resume_job(job_id: str):
        ok, state = queue_manager.resume_job(job_id)
        if not ok:
            if not repo.get_download(job_id):
                return jsonify({"ok": False, "error": "not_found"}), 404
            return jsonify({"ok": False, "error": state}), 409
        job = repo.get_download(job_id)
        return jsonify({"ok": True, "state": state, "job": job})

    @app.route("/api/jobs/<job_id>/retry", methods=["POST"])
    def retry_job(job_id: str):
        ok, state = queue_manager.retry_job(job_id)
        if not ok:
            if not repo.get_download(job_id):
                return jsonify({"ok": False, "error": "not_found"}), 404
            return jsonify({"ok": False, "error": state}), 409
        job = repo.get_download(job_id)
        return jsonify({"ok": True, "state": state, "job": job})

    @app.route("/api/jobs/<job_id>", methods=["DELETE"])
    def delete_job(job_id: str):
        ok, state = queue_manager.delete_job(job_id)
        if not ok:
            return jsonify({"ok": False, "error": state}), 404
        return jsonify({"ok": True, "state": state})

    @app.route("/delete", methods=["POST"])
    def delete_video():
        job_id = (request.form.get("job_id") or "").strip()
        filename = (request.form.get("filename") or "").strip()

        if job_id:
            ok, state = queue_manager.delete_job(job_id)
            if not ok:
                return jsonify({"ok": False, "error": state}), 404
            return jsonify({"ok": True})

        if not filename:
            return jsonify({"ok": False, "error": "No filename"}), 400
        if "/" in filename or "\\" in filename or ".." in filename:
            return jsonify({"ok": False, "error": "Invalid filename"}), 403

        row = repo.get_download_by_filename(filename)
        if not row:
            return jsonify({"ok": False, "error": "File not found"}), 404

        ok, state = queue_manager.delete_job(str(row.get("id")))
        if not ok:
            return jsonify({"ok": False, "error": state}), 500
        return jsonify({"ok": True})

    @app.route("/api/presets", methods=["GET"])
    def presets():
        return jsonify(
            {
                "ok": True,
                "presets": [
                    {"id": preset_id, "label": cfg["label"]}
                    for preset_id, cfg in PRESET_CONFIG.items()
                ],
                "default": "best",
            }
        )

    @app.route("/api/probe", methods=["GET"])
    def api_probe():
        url = (request.args.get("u") or "").strip()
        if not _is_valid_url(url):
            return jsonify({"ok": False, "error": "invalid_url"}), 400

        opts = {"skip_download": True, "quiet": True}
        try:
            with YoutubeDL(opts) as ydl:
                info = ydl.extract_info(url, download=False)
            return jsonify(
                {
                    "ok": True,
                    "title": info.get("title"),
                    "ext": info.get("ext"),
                    "id": info.get("id"),
                    "uploader": info.get("uploader"),
                }
            )
        except Exception as exc:
            return jsonify({"ok": False, "error": str(exc)}), 500

    @app.route("/metrics", methods=["GET"])
    def metrics_endpoint():
        return metrics_response()

    @app.route("/readyz", methods=["GET"])
    def readyz():
        checks: dict[str, object] = {
            "db": False,
            "download_dir_writable": False,
            "disk_free_mb": None,
            "required_min_disk_free_mb": app.config["MIN_FREE_DISK_MB"],
        }

        try:
            checks["db"] = repo.check_read_write()
        except Exception as exc:
            checks["db_error"] = str(exc)

        try:
            checks["download_dir_writable"] = os.access(app.config["BASE_DOWNLOAD_DIR"], os.W_OK)
        except Exception as exc:
            checks["download_dir_error"] = str(exc)

        try:
            usage = shutil.disk_usage(app.config["BASE_DOWNLOAD_DIR"])
            free_mb = int(usage.free / (1024 * 1024))
            checks["disk_free_mb"] = free_mb
            checks["disk_ok"] = free_mb >= int(app.config["MIN_FREE_DISK_MB"])
        except Exception as exc:
            checks["disk_error"] = str(exc)
            checks["disk_ok"] = False

        ok = bool(checks.get("db") and checks.get("download_dir_writable") and checks.get("disk_ok"))
        status = 200 if ok else 503
        return jsonify({"ok": ok, "checks": checks}), status

    @app.route("/healthz", methods=["GET"])
    def health():
        return "ok", 200

    @app.route("/<path:raw>", methods=["GET"])
    def catch_all(raw: str):
        if raw.startswith(("api/", "download", "healthz", "static/", "metrics", "readyz", "gallery", "files/")):
            return render_template("index.html", error="Pfad nicht gefunden.", url="", preset_options=PRESET_CONFIG, default_preset="best"), 404

        candidate = _normalize_external_url(raw)
        if not candidate:
            return (
                render_template(
                    "index.html",
                    error="Ungültige URL.",
                    url=raw,
                    preset_options=PRESET_CONFIG,
                    default_preset="best",
                ),
                400,
            )

        record = _enqueue_download(candidate, "best")
        return render_template(
            "index.html",
            url=candidate,
            job_id=record.get("id"),
            feedback="✅ Download gestartet (läuft im Hintergrund).",
            preset_options=PRESET_CONFIG,
            default_preset="best",
        )

    return app


app = create_app()


if __name__ == "__main__":
    print("[WARNING] Starting Flask Development Server. Do not use in production.")
    app.run(host="0.0.0.0", port=8000)
