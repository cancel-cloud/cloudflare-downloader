import json
import os
import sqlite3
from datetime import datetime, timezone
from typing import Any

from .db import get_connection, init_db


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class DownloadRepository:
    def __init__(self, sqlite_path: str):
        self.sqlite_path = sqlite_path

    def init(self) -> None:
        init_db(self.sqlite_path)

    def _fetchone(self, query: str, params: tuple[Any, ...] = ()) -> dict[str, Any] | None:
        with get_connection(self.sqlite_path) as connection:
            row = connection.execute(query, params).fetchone()
            return self._row_to_dict(row)

    def _execute(self, query: str, params: tuple[Any, ...] = ()) -> int:
        with get_connection(self.sqlite_path) as connection:
            cursor = connection.execute(query, params)
            return cursor.rowcount

    def _row_to_dict(self, row: sqlite3.Row | None) -> dict[str, Any] | None:
        if row is None:
            return None
        data = dict(row)
        metadata_json = data.get("metadata_json")
        if metadata_json:
            try:
                data["metadata"] = json.loads(metadata_json)
            except json.JSONDecodeError:
                data["metadata"] = None
        else:
            data["metadata"] = None
        return data

    @staticmethod
    def _to_json_string(value: dict[str, Any]) -> str:
        try:
            return json.dumps(value, ensure_ascii=False)
        except TypeError:
            return json.dumps(value, ensure_ascii=False, default=str)

    def create_download(self, download_id: str, requested_url: str, preset: str) -> dict[str, Any]:
        now = utc_now_iso()
        with get_connection(self.sqlite_path) as connection:
            connection.execute(
                """
                INSERT INTO downloads (
                    id, requested_url, canonical_url, preset, status,
                    progress_percent, downloaded_bytes, total_bytes,
                    speed_bps, eta_seconds, attempt_current, attempt_max,
                    created_at, queued_at, updated_at
                )
                VALUES (?, ?, ?, ?, 'queued', 0, 0, NULL, NULL, NULL, 0, 1, ?, ?, ?)
                """,
                (download_id, requested_url, requested_url, preset, now, now, now),
            )
        return self.get_download(download_id) or {}

    def get_download(self, download_id: str) -> dict[str, Any] | None:
        return self._fetchone("SELECT * FROM downloads WHERE id = ?", (download_id,))

    def get_download_by_filename(self, filename: str) -> dict[str, Any] | None:
        return self._fetchone(
            """
            SELECT * FROM downloads
            WHERE media_local_path = ?
               OR media_local_path LIKE ?
               OR thumbnail_local_path = ?
               OR thumbnail_local_path LIKE ?
            ORDER BY created_at DESC
            LIMIT 1
            """,
            (filename, f"%/{filename}", filename, f"%/{filename}"),
        )

    def recover_interrupted_downloads(self) -> int:
        now = utc_now_iso()
        return self._execute(
            """
            UPDATE downloads
            SET status = 'failed',
                error_message = 'interrupted_by_restart',
                failed_at = ?,
                updated_at = ?
            WHERE status = 'downloading'
            """,
            (now, now),
        )

    def get_queued_ids(self, limit: int) -> list[str]:
        with get_connection(self.sqlite_path) as connection:
            rows = connection.execute(
                "SELECT id FROM downloads WHERE status = 'queued' ORDER BY created_at ASC LIMIT ?",
                (limit,),
            ).fetchall()
        return [row["id"] for row in rows]

    def update_fields(self, download_id: str, **fields: Any) -> bool:
        if not fields:
            return False
        fields["updated_at"] = utc_now_iso()
        assignments = ", ".join(f"{key} = ?" for key in fields)
        values = list(fields.values())
        values.append(download_id)
        updated = self._execute(f"UPDATE downloads SET {assignments} WHERE id = ?", tuple(values))
        return updated > 0

    def set_downloading(self, download_id: str, attempt_current: int, attempt_max: int, runtime_profile: str) -> bool:
        now = utc_now_iso()
        with get_connection(self.sqlite_path) as connection:
            row = connection.execute(
                """
                UPDATE downloads
                SET status = 'downloading',
                    started_at = COALESCE(started_at, ?),
                    attempt_current = ?,
                    attempt_max = ?,
                    runtime_profile = ?,
                    error_message = NULL,
                    last_exception_type = NULL,
                    updated_at = ?
                WHERE id = ?
                  AND status IN ('queued', 'retrying')
                """,
                (now, attempt_current, attempt_max, runtime_profile, now, download_id),
            )
        return row.rowcount > 0

    def pause_queued(self, download_id: str) -> bool:
        now = utc_now_iso()
        return (
            self._execute(
                """
                UPDATE downloads
                SET status = 'paused',
                    paused_at = ?,
                    updated_at = ?
                WHERE id = ?
                  AND status = 'queued'
                """,
                (now, now, download_id),
            )
            > 0
        )

    def resume_paused(self, download_id: str) -> bool:
        now = utc_now_iso()
        return (
            self._execute(
                """
                UPDATE downloads
                SET status = 'queued',
                    queued_at = ?,
                    paused_at = NULL,
                    error_message = NULL,
                    eta_seconds = NULL,
                    speed_bps = NULL,
                    updated_at = ?
                WHERE id = ?
                  AND status = 'paused'
                """,
                (now, now, download_id),
            )
            > 0
        )

    def retry_download(self, download_id: str) -> bool:
        now = utc_now_iso()
        return (
            self._execute(
                """
                UPDATE downloads
                SET status = 'queued',
                    queued_at = ?,
                    paused_at = NULL,
                    failed_at = NULL,
                    completed_at = NULL,
                    error_message = NULL,
                    last_exception_type = NULL,
                    progress_percent = 0,
                    downloaded_bytes = 0,
                    total_bytes = NULL,
                    speed_bps = NULL,
                    eta_seconds = NULL,
                    attempt_current = 0,
                    attempt_max = attempt_max + 1,
                    updated_at = ?
                WHERE id = ?
                  AND status IN ('failed', 'paused')
                """,
                (now, now, download_id),
            )
            > 0
        )

    def set_paused(self, download_id: str, message: str = "paused_by_user") -> bool:
        now = utc_now_iso()
        return (
            self._execute(
                """
                UPDATE downloads
                SET status = 'paused',
                    paused_at = ?,
                    error_message = ?,
                    speed_bps = NULL,
                    eta_seconds = NULL,
                    updated_at = ?
                WHERE id = ?
                """,
                (now, message, now, download_id),
            )
            > 0
        )

    def set_failed(
        self,
        download_id: str,
        message: str,
        exception_type: str,
        runtime_profile: str,
        attempt_current: int,
        attempt_max: int,
    ) -> bool:
        now = utc_now_iso()
        return (
            self._execute(
                """
                UPDATE downloads
                SET status = 'failed',
                    error_message = ?,
                    last_exception_type = ?,
                    runtime_profile = ?,
                    attempt_current = ?,
                    attempt_max = ?,
                    failed_at = ?,
                    speed_bps = NULL,
                    eta_seconds = NULL,
                    updated_at = ?
                WHERE id = ?
                """,
                (
                    message,
                    exception_type,
                    runtime_profile,
                    attempt_current,
                    attempt_max,
                    now,
                    now,
                    download_id,
                ),
            )
            > 0
        )

    def set_completed(
        self,
        download_id: str,
        runtime_profile: str,
        attempt_current: int,
        attempt_max: int,
        info: dict[str, Any] | None,
        media_local_path: str | None,
        thumbnail_local_path: str | None,
    ) -> bool:
        info = info or {}
        now = utc_now_iso()
        metadata_json = self._to_json_string(info)
        return (
            self._execute(
                """
                UPDATE downloads
                SET status = 'completed',
                    runtime_profile = ?,
                    attempt_current = ?,
                    attempt_max = ?,
                    webpage_url = ?,
                    extractor = ?,
                    extractor_key = ?,
                    video_id = ?,
                    title = ?,
                    uploader = ?,
                    uploader_id = ?,
                    channel = ?,
                    channel_id = ?,
                    duration_seconds = ?,
                    upload_date = ?,
                    thumbnail_remote_url = ?,
                    thumbnail_local_path = ?,
                    media_local_path = ?,
                    media_ext = ?,
                    progress_percent = 100,
                    speed_bps = NULL,
                    eta_seconds = NULL,
                    error_message = NULL,
                    last_exception_type = NULL,
                    metadata_json = ?,
                    completed_at = ?,
                    updated_at = ?
                WHERE id = ?
                """,
                (
                    runtime_profile,
                    attempt_current,
                    attempt_max,
                    info.get("webpage_url"),
                    info.get("extractor"),
                    info.get("extractor_key"),
                    info.get("id"),
                    info.get("title"),
                    info.get("uploader"),
                    info.get("uploader_id"),
                    info.get("channel"),
                    info.get("channel_id"),
                    info.get("duration"),
                    info.get("upload_date"),
                    info.get("thumbnail"),
                    thumbnail_local_path,
                    media_local_path,
                    os.path.splitext(media_local_path or "")[1].lstrip(".") or info.get("ext"),
                    metadata_json,
                    now,
                    now,
                    download_id,
                ),
            )
            > 0
        )

    def update_progress(
        self,
        download_id: str,
        progress_percent: float | None,
        downloaded_bytes: int | None,
        total_bytes: int | None,
        speed_bps: float | None,
        eta_seconds: int | None,
    ) -> bool:
        return (
            self._execute(
                """
                UPDATE downloads
                SET progress_percent = ?,
                    downloaded_bytes = ?,
                    total_bytes = ?,
                    speed_bps = ?,
                    eta_seconds = ?,
                    updated_at = ?
                WHERE id = ?
                """,
                (
                    progress_percent,
                    downloaded_bytes,
                    total_bytes,
                    speed_bps,
                    eta_seconds,
                    utc_now_iso(),
                    download_id,
                ),
            )
            > 0
        )

    def create_attempt(self, download_id: str, attempt_no: int, runtime_profile: str) -> int:
        with get_connection(self.sqlite_path) as connection:
            cursor = connection.execute(
                """
                INSERT INTO download_attempts (download_id, attempt_no, runtime_profile, status, started_at)
                VALUES (?, ?, ?, 'started', ?)
                """,
                (download_id, attempt_no, runtime_profile, utc_now_iso()),
            )
            return int(cursor.lastrowid)

    def finalize_attempt(
        self,
        attempt_id: int,
        status: str,
        error_message: str | None = None,
        exception_type: str | None = None,
    ) -> bool:
        return (
            self._execute(
                """
                UPDATE download_attempts
                SET status = ?,
                    error_message = ?,
                    exception_type = ?,
                    ended_at = ?
                WHERE id = ?
                """,
                (status, error_message, exception_type, utc_now_iso(), attempt_id),
            )
            > 0
        )

    def delete_download(self, download_id: str) -> tuple[bool, dict[str, Any] | None]:
        record = self.get_download(download_id)
        if not record:
            return False, None
        deleted = self._execute("DELETE FROM downloads WHERE id = ?", (download_id,)) > 0
        return deleted, record

    def count_by_status(self, status: str) -> int:
        row = self._fetchone("SELECT COUNT(*) AS cnt FROM downloads WHERE status = ?", (status,))
        return int(row["cnt"]) if row else 0

    def count_queue_depth(self) -> int:
        row = self._fetchone("SELECT COUNT(*) AS cnt FROM downloads WHERE status = 'queued'")
        return int(row["cnt"]) if row else 0

    def list_downloads(
        self,
        page: int,
        per_page: int,
        status: str | None,
        q: str | None,
        sort: str,
        uploader: str | None,
    ) -> tuple[list[dict[str, Any]], int]:
        where: list[str] = []
        params: list[Any] = []

        if status:
            where.append("status = ?")
            params.append(status)
        if uploader:
            where.append("LOWER(COALESCE(uploader, '')) = LOWER(?)")
            params.append(uploader)
        if q:
            where.append(
                "(LOWER(COALESCE(title, '')) LIKE ? OR LOWER(COALESCE(uploader, '')) LIKE ? OR LOWER(COALESCE(video_id, '')) LIKE ?)"
            )
            wildcard = f"%{q.lower()}%"
            params.extend([wildcard, wildcard, wildcard])

        where_clause = f"WHERE {' AND '.join(where)}" if where else ""

        order_by = {
            "created_desc": "created_at DESC",
            "created_asc": "created_at ASC",
            "title_asc": "LOWER(COALESCE(title, '')) ASC, created_at DESC",
            "uploader_asc": "LOWER(COALESCE(uploader, '')) ASC, created_at DESC",
        }.get(sort, "created_at DESC")

        offset = (page - 1) * per_page
        with get_connection(self.sqlite_path) as connection:
            rows = connection.execute(
                f"SELECT * FROM downloads {where_clause} ORDER BY {order_by} LIMIT ? OFFSET ?",
                tuple(params + [per_page, offset]),
            ).fetchall()
            count_row = connection.execute(
                f"SELECT COUNT(*) AS cnt FROM downloads {where_clause}",
                tuple(params),
            ).fetchone()

        total = int(count_row["cnt"]) if count_row else 0
        return [self._row_to_dict(row) or {} for row in rows], total

    def check_read_write(self) -> bool:
        with get_connection(self.sqlite_path) as connection:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS _readyz_probe (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts TEXT NOT NULL
                )
                """
            )
            cursor = connection.execute(
                "INSERT INTO _readyz_probe (ts) VALUES (?)",
                (utc_now_iso(),),
            )
            probe_id = cursor.lastrowid
            connection.execute("DELETE FROM _readyz_probe WHERE id = ?", (probe_id,))
            connection.execute("SELECT 1").fetchone()
        return True
