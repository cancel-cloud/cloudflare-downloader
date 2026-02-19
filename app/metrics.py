import time
from typing import Any

from flask import Response, request
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Gauge, Histogram, generate_latest

HTTP_REQUESTS_TOTAL = Counter(
    "http_requests_total",
    "Total HTTP requests",
    ["method", "route", "status"],
)
HTTP_REQUEST_DURATION_SECONDS = Histogram(
    "http_request_duration_seconds",
    "HTTP request latency seconds",
    ["method", "route"],
)

DOWNLOADER_JOBS_QUEUED_TOTAL = Counter(
    "downloader_jobs_queued_total",
    "Queued jobs",
    ["preset"],
)
DOWNLOADER_JOBS_STARTED_TOTAL = Counter(
    "downloader_jobs_started_total",
    "Started jobs",
    ["preset"],
)
DOWNLOADER_JOBS_COMPLETED_TOTAL = Counter(
    "downloader_jobs_completed_total",
    "Completed jobs",
    ["preset"],
)
DOWNLOADER_JOBS_FAILED_TOTAL = Counter(
    "downloader_jobs_failed_total",
    "Failed jobs",
    ["reason"],
)
DOWNLOADER_JOBS_PAUSED_TOTAL = Counter(
    "downloader_jobs_paused_total",
    "Paused jobs",
)
DOWNLOADER_JOBS_RETRIED_TOTAL = Counter(
    "downloader_jobs_retried_total",
    "Retried jobs",
)
DOWNLOADER_ACTIVE_JOBS = Gauge(
    "downloader_active_jobs",
    "Active download workers",
)
DOWNLOADER_QUEUE_DEPTH = Gauge(
    "downloader_queue_depth",
    "Queued download items",
)
DOWNLOADER_JOB_DURATION_SECONDS = Histogram(
    "downloader_job_duration_seconds",
    "Download processing duration",
    ["preset", "status"],
)
DOWNLOADER_DOWNLOADED_BYTES_TOTAL = Counter(
    "downloader_downloaded_bytes_total",
    "Downloaded bytes",
)


class MetricsRecorder:
    def http_before_request(self) -> float:
        return time.perf_counter()

    def http_after_request(self, started: float, response_status: int) -> None:
        elapsed = max(0.0, time.perf_counter() - started)
        route = request.url_rule.rule if request.url_rule else "unknown"
        method = request.method
        status = str(response_status)
        HTTP_REQUESTS_TOTAL.labels(method=method, route=route, status=status).inc()
        HTTP_REQUEST_DURATION_SECONDS.labels(method=method, route=route).observe(elapsed)

    def mark_queued(self, preset: str) -> None:
        DOWNLOADER_JOBS_QUEUED_TOTAL.labels(preset=preset).inc()

    def mark_started(self, preset: str) -> None:
        DOWNLOADER_JOBS_STARTED_TOTAL.labels(preset=preset).inc()

    def mark_completed(self, preset: str) -> None:
        DOWNLOADER_JOBS_COMPLETED_TOTAL.labels(preset=preset).inc()

    def mark_failed(self, reason: str) -> None:
        DOWNLOADER_JOBS_FAILED_TOTAL.labels(reason=reason or "unknown").inc()

    def mark_paused(self) -> None:
        DOWNLOADER_JOBS_PAUSED_TOTAL.inc()

    def mark_retried(self) -> None:
        DOWNLOADER_JOBS_RETRIED_TOTAL.inc()

    def set_active_jobs(self, value: int) -> None:
        DOWNLOADER_ACTIVE_JOBS.set(max(0, value))

    def set_queue_depth(self, value: int) -> None:
        DOWNLOADER_QUEUE_DEPTH.set(max(0, value))

    def observe_duration(self, preset: str, status: str, seconds: float) -> None:
        DOWNLOADER_JOB_DURATION_SECONDS.labels(preset=preset, status=status).observe(max(0.0, seconds))

    def add_downloaded_bytes(self, value: int) -> None:
        if value > 0:
            DOWNLOADER_DOWNLOADED_BYTES_TOTAL.inc(value)


def metrics_response() -> Response:
    return Response(generate_latest(), mimetype=CONTENT_TYPE_LATEST)
