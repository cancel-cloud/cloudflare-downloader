import glob
import logging
import os
import shutil
import threading
import time
from concurrent.futures import Future, ThreadPoolExecutor
from typing import Any
from urllib.parse import urlparse

from yt_dlp import YoutubeDL
from yt_dlp.utils import DownloadError

from .metrics import MetricsRecorder
from .repository import DownloadRepository

LOGGER = logging.getLogger(__name__)

PRESET_CONFIG: dict[str, dict[str, Any]] = {
    "best": {
        "label": "Best",
        "format": "bestvideo+bestaudio/best",
    },
    "best_1080p": {
        "label": "Best 1080p",
        "format": "bestvideo[height<=1080]+bestaudio/best[height<=1080]/best",
    },
    "audio_only": {
        "label": "Audio only (M4A)",
        "format": "bestaudio/best",
        "audio_only": True,
        "audio_format": "m4a",
    },
}

RETRYABLE_ERROR_TOKENS = (
    "403",
    "forbidden",
    "sabr",
    "missing a url",
    "unable to download video data",
)


class PauseRequestedError(Exception):
    pass


class QueueManager:
    def __init__(
        self,
        repo: DownloadRepository,
        metrics: MetricsRecorder,
        base_download_dir: str,
        max_concurrent_downloads: int,
        progress_flush_interval_ms: int,
    ):
        self.repo = repo
        self.metrics = metrics
        self.base_download_dir = base_download_dir
        self.max_concurrent_downloads = max(1, max_concurrent_downloads)
        self.progress_flush_interval_s = max(0.1, progress_flush_interval_ms / 1000.0)

        self.executor = ThreadPoolExecutor(max_workers=self.max_concurrent_downloads)
        self._active: dict[str, dict[str, Any]] = {}
        self._active_lock = threading.Lock()
        self._stop_event = threading.Event()
        self._scheduler_thread: threading.Thread | None = None

        self.ytdlp_js_runtime = os.environ.get("YTDLP_JS_RUNTIME", "node").strip()
        self.ytdlp_js_runtime_path = os.environ.get("YTDLP_JS_RUNTIME_PATH", "/usr/bin/node").strip()
        self.ytdlp_ffmpeg_path = os.environ.get("YTDLP_FFMPEG_PATH", "").strip()
        self.ytdlp_enable_youtube_fallback = (
            os.environ.get("YTDLP_ENABLE_YOUTUBE_FALLBACK", "1").strip().lower()
            in {"1", "true", "yes", "on"}
        )

    def start(self) -> None:
        if self._scheduler_thread and self._scheduler_thread.is_alive():
            return
        self._stop_event.clear()
        self._scheduler_thread = threading.Thread(target=self._scheduler_loop, daemon=True)
        self._scheduler_thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        if self._scheduler_thread:
            self._scheduler_thread.join(timeout=2)
        self.executor.shutdown(wait=False, cancel_futures=False)

    def _scheduler_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                self.run_once()
            except Exception:
                LOGGER.exception("scheduler_iteration_failed")
            time.sleep(0.5)

    def run_once(self) -> None:
        self._sync_metrics()

        with self._active_lock:
            active_count = len(self._active)
        available = self.max_concurrent_downloads - active_count
        if available <= 0:
            return

        queued_ids = self.repo.get_queued_ids(limit=available)
        for job_id in queued_ids:
            if not self._try_start_job(job_id):
                continue

    def _sync_metrics(self) -> None:
        with self._active_lock:
            self.metrics.set_active_jobs(len(self._active))
        self.metrics.set_queue_depth(self.repo.count_queue_depth())

    def _try_start_job(self, job_id: str) -> bool:
        with self._active_lock:
            if job_id in self._active:
                return False
            cancel_event = threading.Event()
            future = self.executor.submit(self._worker, job_id, cancel_event)
            future.add_done_callback(lambda _f, jid=job_id: self._on_future_done(jid))
            self._active[job_id] = {
                "future": future,
                "cancel_event": cancel_event,
                "started_monotonic": time.monotonic(),
            }
        return True

    def _on_future_done(self, job_id: str) -> None:
        with self._active_lock:
            self._active.pop(job_id, None)
        self._sync_metrics()

    def _worker(self, job_id: str, cancel_event: threading.Event) -> None:
        started = time.monotonic()
        download = self.repo.get_download(job_id)
        if not download:
            return

        preset = download.get("preset") or "best"
        url = str(download.get("requested_url") or "")
        if not url:
            return

        attempts = ["primary"]
        if self.ytdlp_enable_youtube_fallback and self.is_youtube_url(url):
            attempts.append("fallback")

        attempt_max = len(attempts)

        for attempt_no, runtime_profile in enumerate(attempts, start=1):
            if cancel_event.is_set():
                self.repo.set_paused(job_id)
                self.metrics.mark_paused()
                self.metrics.observe_duration(preset, "paused", time.monotonic() - started)
                return

            if not self.repo.set_downloading(job_id, attempt_no, attempt_max, runtime_profile):
                current = self.repo.get_download(job_id)
                if current and current.get("status") == "paused":
                    self.metrics.observe_duration(preset, "paused", time.monotonic() - started)
                return

            self.metrics.mark_started(preset)
            attempt_id = self.repo.create_attempt(job_id, attempt_no, runtime_profile)

            try:
                info = self._download_with_progress(
                    job_id=job_id,
                    url=url,
                    preset=preset,
                    attempt_no=attempt_no,
                    attempt_max=attempt_max,
                    runtime_profile=runtime_profile,
                    cancel_event=cancel_event,
                )
                media_path = self._resolve_media_path(info)
                thumb_path = self._resolve_thumbnail_path(media_path)
                self.repo.set_completed(
                    download_id=job_id,
                    runtime_profile=runtime_profile,
                    attempt_current=attempt_no,
                    attempt_max=attempt_max,
                    info=info,
                    media_local_path=media_path,
                    thumbnail_local_path=thumb_path,
                )
                self.repo.finalize_attempt(attempt_id, "completed")
                self.metrics.mark_completed(preset)
                self.metrics.observe_duration(preset, "completed", time.monotonic() - started)
                LOGGER.info(
                    "job_completed",
                    extra={
                        "job_id": job_id,
                        "preset": preset,
                        "attempt": attempt_no,
                        "runtime_profile": runtime_profile,
                    },
                )
                return
            except PauseRequestedError:
                self.repo.set_paused(job_id)
                self.repo.finalize_attempt(
                    attempt_id,
                    "paused",
                    error_message="paused_by_user",
                    exception_type="PauseRequestedError",
                )
                self.metrics.mark_paused()
                self.metrics.observe_duration(preset, "paused", time.monotonic() - started)
                LOGGER.info("job_paused", extra={"job_id": job_id, "preset": preset})
                return
            except DownloadError as exc:
                message = str(exc)
                self.repo.finalize_attempt(
                    attempt_id,
                    "failed",
                    error_message=message,
                    exception_type="DownloadError",
                )
                if self._is_retryable(message, runtime_profile, attempt_no, attempt_max):
                    continue
                self.repo.set_failed(job_id, message, "DownloadError", runtime_profile, attempt_no, attempt_max)
                self.metrics.mark_failed(self._failure_reason(message))
                self.metrics.observe_duration(preset, "failed", time.monotonic() - started)
                LOGGER.error(
                    "job_failed",
                    extra={
                        "job_id": job_id,
                        "preset": preset,
                        "attempt": attempt_no,
                        "runtime_profile": runtime_profile,
                        "error_type": "DownloadError",
                    },
                )
                return
            except Exception as exc:
                message = str(exc)
                exception_type = type(exc).__name__
                self.repo.finalize_attempt(
                    attempt_id,
                    "failed",
                    error_message=message,
                    exception_type=exception_type,
                )
                if self._is_retryable(message, runtime_profile, attempt_no, attempt_max):
                    continue
                self.repo.set_failed(job_id, message, exception_type, runtime_profile, attempt_no, attempt_max)
                self.metrics.mark_failed(self._failure_reason(message))
                self.metrics.observe_duration(preset, "failed", time.monotonic() - started)
                LOGGER.error(
                    "job_failed",
                    extra={
                        "job_id": job_id,
                        "preset": preset,
                        "attempt": attempt_no,
                        "runtime_profile": runtime_profile,
                        "error_type": exception_type,
                    },
                )
                return

    def pause_job(self, job_id: str) -> tuple[bool, str]:
        if self.repo.pause_queued(job_id):
            self.metrics.mark_paused()
            return True, "paused"

        with self._active_lock:
            handle = self._active.get(job_id)

        if not handle:
            return False, "job_not_active_or_not_queued"

        cancel_event: threading.Event = handle["cancel_event"]
        cancel_event.set()
        self.repo.set_paused(job_id)
        self.metrics.mark_paused()
        LOGGER.info("pause_requested", extra={"job_id": job_id})
        return True, "pause_requested"

    def resume_job(self, job_id: str) -> tuple[bool, str]:
        if not self.repo.resume_paused(job_id):
            return False, "invalid_state"
        LOGGER.info("job_resumed", extra={"job_id": job_id})
        return True, "queued"

    def retry_job(self, job_id: str) -> tuple[bool, str]:
        if not self.repo.retry_download(job_id):
            return False, "invalid_state"
        self.metrics.mark_retried()
        LOGGER.info("job_retried", extra={"job_id": job_id})
        return True, "queued"

    def delete_job(self, job_id: str) -> tuple[bool, str]:
        with self._active_lock:
            handle = self._active.get(job_id)
        if handle:
            handle["cancel_event"].set()

        deleted, record = self.repo.delete_download(job_id)
        if not deleted:
            return False, "not_found"

        self._delete_local_files(record or {})
        LOGGER.info("job_deleted", extra={"job_id": job_id})
        return True, "deleted"

    def _delete_local_files(self, record: dict[str, Any]) -> None:
        media_path = record.get("media_local_path")
        thumb_path = record.get("thumbnail_local_path")

        candidates: set[str] = set()
        if isinstance(media_path, str) and media_path:
            candidates.add(media_path)
            base_no_ext, _ext = os.path.splitext(media_path)
            candidates.update({
                f"{base_no_ext}.info.json",
                f"{base_no_ext}.jpg",
                f"{base_no_ext}.webp",
                f"{base_no_ext}.png",
            })

        if isinstance(thumb_path, str) and thumb_path:
            candidates.add(thumb_path)

        for candidate in candidates:
            full_path = self._safe_storage_path(candidate)
            if not full_path or not os.path.isfile(full_path):
                continue
            try:
                os.remove(full_path)
            except OSError:
                LOGGER.exception("file_delete_failed", extra={"path": candidate})

    def _safe_storage_path(self, relative_path: str | None) -> str | None:
        if not relative_path:
            return None
        normalized = relative_path.replace("\\", "/").lstrip("/")
        full_path = os.path.realpath(os.path.join(self.base_download_dir, normalized))
        base = os.path.realpath(self.base_download_dir)
        if os.path.commonpath([full_path, base]) != base:
            return None
        return full_path

    def _download_with_progress(
        self,
        job_id: str,
        url: str,
        preset: str,
        attempt_no: int,
        attempt_max: int,
        runtime_profile: str,
        cancel_event: threading.Event,
    ) -> dict[str, Any]:
        last_flush = 0.0
        last_bytes = 0

        def progress_hook(progress: dict[str, Any]) -> None:
            nonlocal last_flush, last_bytes
            if cancel_event.is_set():
                raise PauseRequestedError("paused_by_user")

            status = progress.get("status")
            if status not in {"downloading", "finished"}:
                return

            downloaded = int(progress.get("downloaded_bytes") or 0)
            total_raw = progress.get("total_bytes") or progress.get("total_bytes_estimate")
            total = int(total_raw) if total_raw else None
            speed = float(progress.get("speed")) if progress.get("speed") else None
            eta = int(progress.get("eta")) if progress.get("eta") is not None else None

            if status == "finished":
                self.repo.update_progress(job_id, 100.0, downloaded, total or downloaded, None, 0)
                delta = max(0, downloaded - last_bytes)
                self.metrics.add_downloaded_bytes(delta)
                last_bytes = downloaded
                return

            now = time.monotonic()
            if now - last_flush < self.progress_flush_interval_s:
                return

            percent = None
            if total and total > 0:
                percent = round((downloaded / total) * 100, 2)

            self.repo.update_progress(job_id, percent, downloaded, total, speed, eta)
            delta = max(0, downloaded - last_bytes)
            self.metrics.add_downloaded_bytes(delta)
            last_bytes = downloaded
            last_flush = now

            LOGGER.info(
                "job_progress",
                extra={
                    "job_id": job_id,
                    "attempt": attempt_no,
                    "attempt_max": attempt_max,
                    "runtime_profile": runtime_profile,
                    "downloaded_bytes": downloaded,
                    "total_bytes": total,
                    "speed_bps": speed,
                    "eta_seconds": eta,
                },
            )

        options = self._build_ydl_options(url, preset, runtime_profile, progress_hook)
        with YoutubeDL(options) as ydl:
            raw_info = ydl.extract_info(url, download=True)
            if hasattr(ydl, "sanitize_info"):
                info = ydl.sanitize_info(raw_info, remove_private_keys=False)
            else:
                info = raw_info

        if isinstance(info, dict) and info.get("_type") == "playlist":
            entries = info.get("entries") or []
            if entries:
                first = entries[0]
                if isinstance(first, dict):
                    info = first

        return info if isinstance(info, dict) else {}

    def _build_ydl_options(
        self,
        url: str,
        preset: str,
        runtime_profile: str,
        progress_hook,
    ) -> dict[str, Any]:
        preset_cfg = PRESET_CONFIG.get(preset, PRESET_CONFIG["best"])

        options: dict[str, Any] = {
            "outtmpl": os.path.join(self.base_download_dir, "%(title).200B [%(id)s].%(ext)s"),
            "restrictfilenames": True,
            "quiet": True,
            "writethumbnail": True,
            "writeinfojson": True,
            "retries": 3,
            "noprogress": True,
            "concurrent_fragment_downloads": 5,
            "format": preset_cfg["format"],
            "progress_hooks": [progress_hook],
        }

        if preset_cfg.get("audio_only"):
            options["postprocessors"] = [
                {
                    "key": "FFmpegExtractAudio",
                    "preferredcodec": preset_cfg.get("audio_format", "m4a"),
                }
            ]
        else:
            options["merge_output_format"] = "mp4"

        if self.ytdlp_js_runtime:
            if self.ytdlp_js_runtime_path:
                options["js_runtimes"] = {self.ytdlp_js_runtime: {"path": self.ytdlp_js_runtime_path}}
            else:
                options["js_runtimes"] = {self.ytdlp_js_runtime: {}}

        if self.ytdlp_ffmpeg_path:
            options["ffmpeg_location"] = self.ytdlp_ffmpeg_path

        if self.is_youtube_url(url) and runtime_profile == "fallback":
            options["extractor_args"] = {
                "youtube": {
                    "player_client": ["android_vr", "android", "ios", "tv"],
                }
            }

        return options

    def _resolve_media_path(self, info: dict[str, Any]) -> str | None:
        requested_downloads = info.get("requested_downloads")
        if isinstance(requested_downloads, list):
            for item in requested_downloads:
                if not isinstance(item, dict):
                    continue
                path = item.get("filepath") or item.get("_filename")
                relative = self._to_relative(path)
                if relative and self._safe_storage_path(relative) and os.path.isfile(self._safe_storage_path(relative) or ""):
                    return relative

        direct_candidates = [
            info.get("filepath"),
            info.get("_filename"),
            info.get("filename"),
        ]
        for path in direct_candidates:
            relative = self._to_relative(path)
            if relative and self._safe_storage_path(relative) and os.path.isfile(self._safe_storage_path(relative) or ""):
                return relative

        video_id = info.get("id")
        if not video_id:
            return None

        pattern = os.path.join(self.base_download_dir, f"*[{video_id}].*")
        candidates = []
        for path in glob.glob(pattern):
            ext = os.path.splitext(path)[1].lower()
            if ext in {".json", ".part", ".ytdl", ".tmp", ".jpg", ".webp", ".png"}:
                continue
            if path.endswith(".info.json"):
                continue
            if os.path.isfile(path):
                candidates.append(path)

        if not candidates:
            return None

        candidates.sort(key=lambda item: os.path.getmtime(item), reverse=True)
        return self._to_relative(candidates[0])

    def _resolve_thumbnail_path(self, media_relative_path: str | None) -> str | None:
        if not media_relative_path:
            return None
        base, _ext = os.path.splitext(media_relative_path)
        for ext in (".jpg", ".webp", ".png"):
            candidate = f"{base}{ext}"
            full = self._safe_storage_path(candidate)
            if full and os.path.isfile(full):
                return candidate
        return None

    def _to_relative(self, path: Any) -> str | None:
        if not isinstance(path, str) or not path:
            return None
        if os.path.isabs(path):
            full = os.path.realpath(path)
            base = os.path.realpath(self.base_download_dir)
            if os.path.commonpath([full, base]) != base:
                return None
            return os.path.relpath(full, base)
        return path.lstrip("/")

    def _is_retryable(self, message: str, runtime_profile: str, attempt_no: int, attempt_max: int) -> bool:
        if runtime_profile != "primary" or attempt_no >= attempt_max:
            return False
        lowered = message.lower()
        return any(token in lowered for token in RETRYABLE_ERROR_TOKENS)

    def _failure_reason(self, message: str) -> str:
        lowered = message.lower()
        if "403" in lowered or "forbidden" in lowered:
            return "forbidden"
        if "network" in lowered:
            return "network"
        if "not available" in lowered:
            return "unavailable"
        return "other"

    @staticmethod
    def is_youtube_url(url: str) -> bool:
        host = (urlparse(url).hostname or "").lower()
        return "youtube.com" in host or "youtu.be" in host


def build_runtime_diagnostics(base_download_dir: str, max_concurrent_downloads: int) -> dict[str, Any]:
    runtime_name = os.environ.get("YTDLP_JS_RUNTIME", "node").strip()
    runtime_path = os.environ.get("YTDLP_JS_RUNTIME_PATH", "/usr/bin/node").strip()
    ffmpeg_path = os.environ.get("YTDLP_FFMPEG_PATH", "").strip() or shutil.which("ffmpeg") or "not_found"

    if runtime_name:
        resolved_runtime_path = runtime_path or shutil.which(runtime_name) or "not_found"
    else:
        resolved_runtime_path = "disabled"

    return {
        "js_runtime": runtime_name or "disabled",
        "configured_runtime_path": runtime_path or "-",
        "resolved_runtime_path": resolved_runtime_path,
        "ffmpeg": ffmpeg_path,
        "max_concurrent_downloads": max_concurrent_downloads,
        "base_download_dir": base_download_dir,
    }
