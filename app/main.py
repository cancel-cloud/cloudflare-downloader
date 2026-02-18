import os
import threading
import uuid
import time
import concurrent.futures
from datetime import datetime
from urllib.parse import unquote
import json
import glob
from flask import Flask, request, render_template, jsonify, send_from_directory, abort
from yt_dlp import YoutubeDL

app = Flask(__name__)

# Zielverzeichnis (per Docker-Volume gemountet)
BASE_DOWNLOAD_DIR = os.environ.get("BASE_DOWNLOAD_DIR", "/data")
os.makedirs(BASE_DOWNLOAD_DIR, exist_ok=True)

# Thread-Pool für parallele Downloads
MAX_CONCURRENT_DOWNLOADS = int(os.environ.get("MAX_CONCURRENT_DOWNLOADS", 4))
executor = concurrent.futures.ThreadPoolExecutor(max_workers=MAX_CONCURRENT_DOWNLOADS)

# Job-Status-Tracking (in-memory, für Production Redis/DB nutzen)
job_status = {}
job_lock = threading.Lock()

# yt-dlp: immer höchste Qualität
YDL_OPTS = {
    "outtmpl": os.path.join(BASE_DOWNLOAD_DIR, "%(title).200B [%(id)s].%(ext)s"),
    "restrictfilenames": True,
    "quiet": True,
    "writethumbnail": True,
    "writeinfojson": True,
    "merge_output_format": "mp4",
    "format": "bestvideo+bestaudio/best",
    "ignoreerrors": True,
    "retries": 3,
    "noprogress": True,
    "concurrent_fragment_downloads": 5
}

def extract_info(url: str):
    opts = dict(YDL_OPTS)
    opts["skip_download"] = True
    with YoutubeDL(opts) as ydl:
        return ydl.extract_info(url, download=False)

def download_video(url: str, job_id: str):
    opts = dict(YDL_OPTS)
    with YoutubeDL(opts) as ydl:
        ydl.download([url])

def update_job_status(job_id: str, status: str, **kwargs):
    with job_lock:
        if job_id not in job_status:
            job_status[job_id] = {}
        job_status[job_id].update({"status": status, "updated": datetime.utcnow().isoformat(), **kwargs})

def process_download(url: str, job_id: str):
    """Führt den Download im ThreadPool aus"""
    try:
        print(f"[JOB] Starting: {url} (Job: {job_id})")
        update_job_status(job_id, "downloading", url=url)
        
        download_video(url, job_id)
        
        update_job_status(job_id, "completed", url=url)
        print(f"[JOB] Done: {url}")
    except Exception as e:
        print(f"[JOB ERROR] {url}: {e}")
        update_job_status(job_id, "failed", url=url, error=str(e))

def cleanup_jobs():
    """Löscht alte Jobs regelmäßig aus dem Speicher"""
    while True:
        time.sleep(3600)  # Alle 60 Minuten
        cutoff = datetime.utcnow().timestamp() - 86400  # 24 Stunden
        
        with job_lock:
            to_delete = []
            for jid, info in job_status.items():
                updated_str = info.get("updated")
                if updated_str:
                    try:
                        updated_ts = datetime.fromisoformat(updated_str).timestamp()
                        if updated_ts < cutoff:
                            to_delete.append(jid)
                    except ValueError:
                        pass
            
            for jid in to_delete:
                del job_status[jid]
            
            if to_delete:
                print(f"[CLEANUP] Removed {len(to_delete)} old jobs.")

# Cleanup-Thread starten
threading.Thread(target=cleanup_jobs, daemon=True).start()

@app.route("/", methods=["GET"])
def index():
    url = (request.args.get("u") or "").strip()
    return render_template("index.html", url=url)

@app.route("/gallery", methods=["GET"])
def gallery():
    """Zeigt die Galerie der heruntergeladenen Videos"""
    videos = []
    
    # Alle .mp4 Dateien finden
    # Wir nehmen an: Titel [id].mp4
    # Zugehörige Info: Titel [id].info.json
    # Zugehörige Thumbnail: Titel [id].jpg (oder .webp)
    
    files = glob.glob(os.path.join(BASE_DOWNLOAD_DIR, "*.mp4"))
    
    for mp4_path in files:
        filename = os.path.basename(mp4_path)
        base_name = os.path.splitext(filename)[0] # remove .mp4
        
        info_path = os.path.join(BASE_DOWNLOAD_DIR, f"{base_name}.info.json")
        thumb_jpg = f"{base_name}.jpg"
        thumb_webp = f"{base_name}.webp"
        
        video_data = {
            "filename": filename,
            "title": filename,
            "uploader": "Unknown",
            "thumbnail": None,
            "id": None,
            "original_url": None
        }
        
        # Versuche Metadaten zu laden
        if os.path.exists(info_path):
            try:
                with open(info_path, "r", encoding="utf-8") as f:
                    info = json.load(f)
                    video_data["title"] = info.get("title", filename)
                    video_data["uploader"] = info.get("uploader", "Unknown")
                    video_data["id"] = info.get("id")
                    video_data["original_url"] = info.get("webpage_url")
            except Exception as e:
                print(f"Error reading info json for {filename}: {e}")
        
        # Check thumbnail
        if os.path.exists(os.path.join(BASE_DOWNLOAD_DIR, thumb_jpg)):
            video_data["thumbnail"] = thumb_jpg
        elif os.path.exists(os.path.join(BASE_DOWNLOAD_DIR, thumb_webp)):
            video_data["thumbnail"] = thumb_webp
            
        videos.append(video_data)
    
    # Sortieren nach Erstelldatum (neueste zuerst)
    videos.sort(key=lambda x: os.path.getctime(os.path.join(BASE_DOWNLOAD_DIR, x["filename"])), reverse=True)
    
    return render_template("gallery.html", videos=videos)

@app.route("/files/<path:filename>")
def serve_file(filename):
    """Serviert heruntergeladene Dateien"""
    return send_from_directory(BASE_DOWNLOAD_DIR, filename)

@app.route("/delete", methods=["POST"])
def delete_video():
    """Löscht ein Video und dessen zugehörige Dateien"""
    filename = request.form.get("filename")
    if not filename:
        return jsonify({"ok": False, "error": "No filename"}), 400
    
    # Security check: ensure traversal attacks are prevented
    # secure_filename von werkzeug nutzen wir hier nicht direkt, da der Dateiname Spaces enthalten kann
    # Aber wir prüfen, dass der Pfad im BASE_DOWNLOAD_DIR liegt
    
    full_path = os.path.join(BASE_DOWNLOAD_DIR, filename)
    if not os.path.commonpath([full_path, BASE_DOWNLOAD_DIR]) == BASE_DOWNLOAD_DIR:
         return jsonify({"ok": False, "error": "Invalid path"}), 403
         
    if not os.path.exists(full_path):
        return jsonify({"ok": False, "error": "File not found"}), 404

    try:
        # Lösche Video
        os.remove(full_path)
        
        # Lösche Nebendateien (info.json, thumbnail)
        base_name = os.path.splitext(filename)[0]
        for ext in [".info.json", ".jpg", ".webp"]:
            sidecar = os.path.join(BASE_DOWNLOAD_DIR, base_name + ext)
            if os.path.exists(sidecar):
                os.remove(sidecar)
                
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

@app.route("/download", methods=["POST"])
def download_route():
    """Nimmt Download-Request entgegen und gibt SOFORT Antwort zurück"""
    url = (request.form.get("u") or "").strip()
    if not (url.startswith("http://") or url.startswith("https://")):
        return jsonify({"ok": False, "error": "Ungültige URL"}), 400
    
    # Job-ID generieren
    job_id = str(uuid.uuid4())
    
    # In ThreadPool übergeben
    update_job_status(job_id, "queued", url=url)
    executor.submit(process_download, url, job_id)
    
    # SOFORT zurückgeben (ohne auf Download zu warten)
    return jsonify({
        "ok": True, 
        "job_id": job_id,
        "msg": "Download gestartet (läuft im Hintergrund)."
    }), 202  # 202 Accepted = Request angenommen, wird asynchron verarbeitet

@app.route("/api/status/<job_id>", methods=["GET"])
def get_job_status(job_id: str):
    """Gibt Status eines Jobs zurück"""
    with job_lock:
        if job_id not in job_status:
            return jsonify({"ok": False, "error": "Job nicht gefunden"}), 404
        return jsonify({"ok": True, "job": job_status[job_id]})

@app.route("/api/probe", methods=["GET"])
def api_probe():
    """Gibt Video-Infos zurück (kann langsam sein, daher optional)"""
    url = (request.args.get("u") or "").strip()
    if not (url.startswith("http://") or url.startswith("https://")):
        return jsonify({"ok": False, "error": "invalid_url"}), 400
    try:
        info = extract_info(url)
        return jsonify({"ok": True, "title": info.get("title"), "ext": info.get("ext")})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

# 🔥 Catch-all: erlaubt Aufruf wie dl.devbrew.dev/https://youtu.be/...
@app.route("/<path:raw>", methods=["GET"])
def catch_all(raw: str):
    # Reservierte Prefixe ignorieren
    if raw.startswith(("api/", "download", "healthz", "static/")):
        return render_template("index.html", error="Pfad nicht gefunden.", url=""), 404

    candidate = _normalize_external_url(raw)
    if not candidate:
        return render_template("index.html", error="Ungültige URL.", url=raw), 400

    # Job erstellen und SOFORT zurückgeben
    job_id = str(uuid.uuid4())
    update_job_status(job_id, "queued", url=candidate)
    executor.submit(process_download, candidate, job_id)

    # Seite mit Feedback laden (OHNE auf download zu warten)
    return render_template("index.html", 
                          url=candidate,
                          job_id=job_id,
                          feedback="✅ Download gestartet (läuft im Hintergrund).")

@app.route("/healthz")
def health():
    return "ok", 200

def _normalize_external_url(raw: str) -> str | None:
    """Rekonstruiert aus Pfad + Query eine vollqualifizierte URL"""
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

if __name__ == "__main__":
    print("[WARNING] Starting Flask Development Server. Do not use in production.")
    app.run(host="0.0.0.0", port=8000)