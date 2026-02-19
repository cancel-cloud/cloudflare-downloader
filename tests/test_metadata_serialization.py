import json


class NonSerializableValue:
    def __str__(self):
        return "NonSerializableValue"


def test_set_completed_serializes_non_json_metadata(repo):
    repo.create_download("job-meta", "https://youtube.com/watch?v=job-meta", "best")

    completed = repo.set_completed(
        download_id="job-meta",
        runtime_profile="primary",
        attempt_current=1,
        attempt_max=1,
        info={
            "id": "job-meta",
            "title": "Meta Test",
            "uploader": "Creator",
            "webpage_url": "https://youtube.com/watch?v=job-meta",
            "postprocessor": NonSerializableValue(),
        },
        media_local_path="Meta Test [job-meta].mp4",
        thumbnail_local_path="Meta Test [job-meta].jpg",
    )

    assert completed is True

    row = repo.get_download("job-meta")
    assert row is not None
    assert row["status"] == "completed"
    assert isinstance(row["metadata_json"], str)

    parsed = json.loads(row["metadata_json"])
    assert parsed["id"] == "job-meta"
    assert parsed["postprocessor"] == "NonSerializableValue"
