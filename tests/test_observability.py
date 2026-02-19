def test_metrics_endpoint_exposes_expected_metrics(client):
    client.get("/healthz")
    client.post("/download", data={"u": "https://youtube.com/watch?v=metric1"})

    response = client.get("/metrics")
    body = response.data.decode("utf-8")

    assert response.status_code == 200
    assert "downloader_jobs_queued_total" in body
    assert "downloader_jobs_started_total" in body
    assert "downloader_jobs_completed_total" in body
    assert "downloader_jobs_failed_total" in body
    assert "downloader_jobs_paused_total" in body
    assert "downloader_jobs_retried_total" in body
    assert "downloader_active_jobs" in body
    assert "downloader_queue_depth" in body
    assert "downloader_job_duration_seconds" in body
    assert "downloader_downloaded_bytes_total" in body
    assert "http_requests_total" in body
    assert "http_request_duration_seconds" in body


def test_readyz_success_and_failure(client, repo, monkeypatch):
    ok_response = client.get("/readyz")
    assert ok_response.status_code == 200
    assert ok_response.get_json()["ok"] is True

    monkeypatch.setattr(repo, "check_read_write", lambda: (_ for _ in ()).throw(RuntimeError("db down")))

    failed_response = client.get("/readyz")
    payload = failed_response.get_json()
    assert failed_response.status_code == 503
    assert payload["ok"] is False
    assert "db_error" in payload["checks"]
