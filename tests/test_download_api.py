def test_download_creates_queued_row_with_default_preset(client, repo):
    response = client.post(
        "/download",
        data={"u": "https://youtube.com/watch?v=abc123"},
    )

    assert response.status_code == 202
    payload = response.get_json()
    assert payload["ok"] is True
    assert payload["preset"] == "best"
    assert payload["status"] == "queued"

    row = repo.get_download(payload["job_id"])
    assert row is not None
    assert row["preset"] == "best"
    assert row["status"] == "queued"


def test_download_rejects_invalid_preset(client):
    response = client.post(
        "/download",
        data={"u": "https://youtube.com/watch?v=abc123", "preset": "not_a_preset"},
    )

    assert response.status_code == 400
    payload = response.get_json()
    assert payload["ok"] is False
    assert payload["error"] == "invalid_preset"


def test_presets_endpoint(client):
    response = client.get("/api/presets")

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["ok"] is True
    ids = {item["id"] for item in payload["presets"]}
    assert {"best", "best_1080p", "audio_only"}.issubset(ids)
