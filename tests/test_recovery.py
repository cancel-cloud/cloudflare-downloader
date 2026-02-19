def test_recovery_marks_stale_downloading_jobs_failed(app_factory):
    app1 = app_factory({"START_QUEUE_MANAGER": False})
    repo1 = app1.extensions["repo"]

    repo1.create_download("recover1", "https://youtube.com/watch?v=recover1", "best")
    repo1.update_fields("recover1", status="downloading")

    app2 = app_factory({"START_QUEUE_MANAGER": False})
    repo2 = app2.extensions["repo"]
    row = repo2.get_download("recover1")

    assert row is not None
    assert row["status"] == "failed"
    assert row["error_message"] == "interrupted_by_restart"
