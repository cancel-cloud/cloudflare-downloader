import threading


def test_pause_resume_retry_flow(client, repo):
    response = client.post("/download", data={"u": "https://youtube.com/watch?v=queued1"})
    job_id = response.get_json()["job_id"]

    pause_response = client.post(f"/api/jobs/{job_id}/pause")
    assert pause_response.status_code == 200
    assert pause_response.get_json()["ok"] is True
    assert repo.get_download(job_id)["status"] == "paused"

    resume_response = client.post(f"/api/jobs/{job_id}/resume")
    assert resume_response.status_code == 200
    assert resume_response.get_json()["ok"] is True
    assert repo.get_download(job_id)["status"] == "queued"

    repo.update_fields(job_id, status="failed", error_message="boom", failed_at="2026-01-01T00:00:00+00:00")
    previous_attempt_max = int(repo.get_download(job_id)["attempt_max"])

    retry_response = client.post(f"/api/jobs/{job_id}/retry")
    assert retry_response.status_code == 200
    assert retry_response.get_json()["ok"] is True

    row = repo.get_download(job_id)
    assert row["status"] == "queued"
    assert int(row["attempt_max"]) == previous_attempt_max + 1


def test_pause_active_job_sets_cancel_event(app, client, repo, queue_manager):
    job = repo.create_download("job-active", "https://youtube.com/watch?v=active", "best")
    repo.update_fields(job["id"], status="downloading")

    cancel_event = threading.Event()
    with queue_manager._active_lock:
        queue_manager._active[job["id"]] = {
            "future": None,
            "cancel_event": cancel_event,
            "started_monotonic": 0,
        }

    response = client.post(f"/api/jobs/{job['id']}/pause")
    payload = response.get_json()

    assert response.status_code == 200
    assert payload["ok"] is True
    assert cancel_event.is_set() is True
    assert repo.get_download(job["id"])["status"] == "paused"
