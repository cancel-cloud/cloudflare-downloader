from pathlib import Path


def test_path_traversal_rejected(client):
    response = client.post("/delete", data={"filename": "../../etc/passwd"})
    assert response.status_code == 403

    file_response = client.get("/files/../../etc/passwd")
    assert file_response.status_code == 403


def test_delete_job_removes_db_row_and_local_files(app, client, repo):
    base = Path(app.config["BASE_DOWNLOAD_DIR"])

    media_rel = "Sample [iddel].mp4"
    thumb_rel = "Sample [iddel].jpg"
    info_rel = "Sample [iddel].info.json"

    (base / media_rel).write_text("video", encoding="utf-8")
    (base / thumb_rel).write_text("thumb", encoding="utf-8")
    (base / info_rel).write_text("{}", encoding="utf-8")

    repo.create_download("iddel", "https://youtube.com/watch?v=iddel", "best")
    repo.update_fields(
        "iddel",
        status="completed",
        media_local_path=media_rel,
        thumbnail_local_path=thumb_rel,
        title="Sample",
    )

    response = client.delete("/api/jobs/iddel")
    assert response.status_code == 200
    assert response.get_json()["ok"] is True

    assert repo.get_download("iddel") is None
    assert not (base / media_rel).exists()
    assert not (base / thumb_rel).exists()
    assert not (base / info_rel).exists()
