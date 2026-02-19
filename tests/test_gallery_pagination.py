def _create_completed(repo, job_id, title, uploader):
    repo.create_download(job_id, f"https://youtube.com/watch?v={job_id}", "best")
    repo.update_fields(
        job_id,
        status="completed",
        title=title,
        uploader=uploader,
        video_id=job_id,
        media_local_path=f"{title} [{job_id}].mp4",
    )


def test_gallery_server_side_pagination_search_and_sort(client, repo):
    _create_completed(repo, "id1", "Charlie", "Uploader C")
    _create_completed(repo, "id2", "Alpha", "Uploader A")
    _create_completed(repo, "id3", "Bravo", "Uploader B")

    page1 = client.get("/gallery?status=completed&sort=title_asc&page=1&per_page=2")
    body1 = page1.data.decode("utf-8")
    assert page1.status_code == 200
    assert "Alpha" in body1
    assert "Bravo" in body1
    assert "Charlie" not in body1

    page2 = client.get("/gallery?status=completed&sort=title_asc&page=2&per_page=2")
    body2 = page2.data.decode("utf-8")
    assert page2.status_code == 200
    assert "Charlie" in body2

    search = client.get("/gallery?status=completed&q=brav&sort=title_asc&page=1&per_page=10")
    body3 = search.data.decode("utf-8")
    assert "Bravo" in body3
    assert "Alpha" not in body3

    api = client.get("/api/jobs?status=completed&sort=title_asc&page=1&per_page=2")
    payload = api.get_json()
    assert payload["ok"] is True
    assert payload["total"] == 3
    assert len(payload["items"]) == 2
    assert payload["items"][0]["title"] == "Alpha"
