import os
import sqlite3
from contextlib import contextmanager
from typing import Iterator

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS downloads (
  id TEXT PRIMARY KEY,
  requested_url TEXT NOT NULL,
  canonical_url TEXT,
  webpage_url TEXT,
  extractor TEXT,
  extractor_key TEXT,
  video_id TEXT,
  title TEXT,
  uploader TEXT,
  uploader_id TEXT,
  channel TEXT,
  channel_id TEXT,
  duration_seconds INTEGER,
  upload_date TEXT,
  thumbnail_remote_url TEXT,
  thumbnail_local_path TEXT,
  media_local_path TEXT,
  media_ext TEXT,
  preset TEXT NOT NULL,
  status TEXT NOT NULL,
  queue_position INTEGER,
  progress_percent REAL,
  downloaded_bytes INTEGER,
  total_bytes INTEGER,
  speed_bps REAL,
  eta_seconds INTEGER,
  runtime_profile TEXT,
  attempt_current INTEGER NOT NULL DEFAULT 0,
  attempt_max INTEGER NOT NULL DEFAULT 1,
  last_exception_type TEXT,
  error_message TEXT,
  metadata_json TEXT,
  created_at TEXT NOT NULL,
  queued_at TEXT,
  started_at TEXT,
  paused_at TEXT,
  completed_at TEXT,
  failed_at TEXT,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS download_attempts (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  download_id TEXT NOT NULL,
  attempt_no INTEGER NOT NULL,
  runtime_profile TEXT,
  status TEXT NOT NULL,
  error_message TEXT,
  exception_type TEXT,
  started_at TEXT NOT NULL,
  ended_at TEXT,
  FOREIGN KEY(download_id) REFERENCES downloads(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_downloads_status_created ON downloads(status, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_downloads_completed_at ON downloads(completed_at DESC);
CREATE INDEX IF NOT EXISTS idx_downloads_title ON downloads(title);
CREATE INDEX IF NOT EXISTS idx_downloads_uploader ON downloads(uploader);
CREATE INDEX IF NOT EXISTS idx_downloads_video_id ON downloads(video_id);
CREATE INDEX IF NOT EXISTS idx_attempts_download_id ON download_attempts(download_id);
"""


def configure_connection(connection: sqlite3.Connection) -> None:
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA journal_mode=WAL;")
    connection.execute("PRAGMA foreign_keys=ON;")
    connection.execute("PRAGMA synchronous=NORMAL;")


@contextmanager
def get_connection(sqlite_path: str) -> Iterator[sqlite3.Connection]:
    os.makedirs(os.path.dirname(sqlite_path), exist_ok=True)
    connection = sqlite3.connect(sqlite_path, timeout=30.0, check_same_thread=False)
    try:
        configure_connection(connection)
        yield connection
        connection.commit()
    finally:
        connection.close()


def init_db(sqlite_path: str) -> None:
    with get_connection(sqlite_path) as connection:
        connection.executescript(SCHEMA_SQL)
