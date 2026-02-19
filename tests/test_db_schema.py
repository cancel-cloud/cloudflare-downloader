import sqlite3


def test_db_schema_created(app):
    sqlite_path = app.config["SQLITE_PATH"]
    with sqlite3.connect(sqlite_path) as connection:
        tables = {
            row[0]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
        indexes = {
            row[0]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type='index'"
            ).fetchall()
        }

    assert "downloads" in tables
    assert "download_attempts" in tables

    assert "idx_downloads_status_created" in indexes
    assert "idx_downloads_completed_at" in indexes
    assert "idx_downloads_title" in indexes
    assert "idx_downloads_uploader" in indexes
    assert "idx_downloads_video_id" in indexes
    assert "idx_attempts_download_id" in indexes
