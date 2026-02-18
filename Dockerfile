FROM python:3.13-slim as builder

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# ffmpeg wird für yt-dlp benötigt (muxing/merging etc.)
# nodejs wird für yt-dlp als JS-Runtime benötigt (bessere Extraktion)
RUN apt-get update \
    && apt-get install -y --no-install-recommends ffmpeg nodejs curl ca-certificates tini \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app

# Nicht-root Nutzer (optional)
RUN useradd -ms /bin/bash appuser
RUN mkdir -p /data && chown -R appuser:appuser /data /app
USER appuser

ENV BASE_DOWNLOAD_DIR=/data
EXPOSE 8000

ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["gunicorn", "-w", "4", "-b", "0.0.0.0:8000", "app.main:app"]
