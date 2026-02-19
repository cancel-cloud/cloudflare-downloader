import threading
import json


class DummyPostprocessor:
    def __repr__(self):
        return "DummyPostprocessor()"


class FakeYoutubeDL:
    def __init__(self, options):
        self.options = options

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def extract_info(self, url, download=True):
        hook = self.options["progress_hooks"][0]
        hook(
            {
                "status": "downloading",
                "downloaded_bytes": 500,
                "total_bytes": 1000,
                "speed": 250,
                "eta": 2,
            }
        )
        hook(
            {
                "status": "finished",
                "downloaded_bytes": 1000,
                "total_bytes": 1000,
            }
        )
        return {
            "id": "abc123",
            "title": "Video",
            "uploader": "Creator",
            "webpage_url": "https://youtube.com/watch?v=abc123",
            "thumbnail": "https://i.ytimg.com/vi/abc123/default.jpg",
            "postprocessor": DummyPostprocessor(),
        }

    def sanitize_info(self, info_dict, remove_private_keys=False):
        return {
            key: ("DummyPostprocessor()" if isinstance(value, DummyPostprocessor) else value)
            for key, value in info_dict.items()
        }


def test_progress_hook_updates_job_state(monkeypatch, repo, queue_manager):
    monkeypatch.setattr("app.queue_manager.YoutubeDL", FakeYoutubeDL)

    repo.create_download("job-progress", "https://youtube.com/watch?v=abc123", "best")

    info = queue_manager._download_with_progress(
        job_id="job-progress",
        url="https://youtube.com/watch?v=abc123",
        preset="best",
        attempt_no=1,
        attempt_max=1,
        runtime_profile="primary",
        cancel_event=threading.Event(),
    )

    row = repo.get_download("job-progress")
    assert row is not None
    assert float(row["progress_percent"]) == 100.0
    assert int(row["downloaded_bytes"]) == 1000
    assert int(row["total_bytes"]) == 1000
    assert info["id"] == "abc123"
    assert info["postprocessor"] == "DummyPostprocessor()"


def test_worker_completes_and_persists_serializable_metadata(monkeypatch, repo, queue_manager):
    monkeypatch.setattr("app.queue_manager.YoutubeDL", FakeYoutubeDL)
    monkeypatch.setattr(queue_manager, "_resolve_media_path", lambda _info: "Video [abc123].mp4")
    monkeypatch.setattr(queue_manager, "_resolve_thumbnail_path", lambda _media: "Video [abc123].jpg")

    repo.create_download("job-worker", "https://youtube.com/watch?v=abc123", "best")

    queue_manager._worker("job-worker", threading.Event())

    row = repo.get_download("job-worker")
    assert row is not None
    assert row["status"] == "completed"
    assert isinstance(row["metadata_json"], str)
    parsed = json.loads(row["metadata_json"])
    assert parsed["id"] == "abc123"
    assert parsed["postprocessor"] == "DummyPostprocessor()"
