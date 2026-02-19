from pathlib import Path

import pytest

from app.main import create_app


@pytest.fixture
def app_factory(tmp_path: Path):
    def _factory(overrides: dict | None = None):
        base_dir = tmp_path / "data"
        base_dir.mkdir(parents=True, exist_ok=True)

        config = {
            "TESTING": True,
            "BASE_DOWNLOAD_DIR": str(base_dir),
            "SQLITE_PATH": str(base_dir / "downloader.db"),
            "START_QUEUE_MANAGER": False,
            "MIN_FREE_DISK_MB": 0,
        }
        if overrides:
            config.update(overrides)

        return create_app(config)

    return _factory


@pytest.fixture
def app(app_factory):
    return app_factory()


@pytest.fixture
def client(app):
    return app.test_client()


@pytest.fixture
def repo(app):
    return app.extensions["repo"]


@pytest.fixture
def queue_manager(app):
    return app.extensions["queue_manager"]
