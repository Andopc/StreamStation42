"""
StreamStation42 - Indie P2P Cable Network

Database layer: creates and manages the SQLite schema for channels,
shows, lineup items, bumpers, commercials and overlays.
"""

import sqlite3
import json
import os
from contextlib import contextmanager

DB_PATH = os.environ.get("SS42_DB_PATH", "streamstation42.db")


def get_db_path() -> str:
    return DB_PATH


@contextmanager
def get_connection(db_path: str = None):
    """Context manager that yields a configured sqlite3 connection."""
    path = db_path or DB_PATH
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_db(db_path: str = None) -> None:
    """Create all tables if they do not yet exist."""
    path = db_path or DB_PATH
    with get_connection(path) as conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS channels (
                id          TEXT PRIMARY KEY,
                name        TEXT NOT NULL,
                description TEXT NOT NULL DEFAULT '',
                owner       TEXT NOT NULL DEFAULT 'anonymous',
                created_at  TEXT NOT NULL,
                config      TEXT NOT NULL DEFAULT '{}'
            );

            CREATE TABLE IF NOT EXISTS shows (
                id              TEXT PRIMARY KEY,
                channel_id      TEXT NOT NULL REFERENCES channels(id) ON DELETE CASCADE,
                title           TEXT NOT NULL,
                description     TEXT NOT NULL DEFAULT '',
                duration        REAL NOT NULL DEFAULT 0,
                file_path       TEXT NOT NULL DEFAULT '',
                torrent_hash    TEXT NOT NULL DEFAULT '',
                torrent_path    TEXT NOT NULL DEFAULT '',
                episode_number  INTEGER NOT NULL DEFAULT 1,
                season_number   INTEGER NOT NULL DEFAULT 1,
                created_at      TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS lineup_items (
                id          TEXT PRIMARY KEY,
                channel_id  TEXT NOT NULL REFERENCES channels(id) ON DELETE CASCADE,
                show_id     TEXT REFERENCES shows(id) ON DELETE SET NULL,
                item_type   TEXT NOT NULL DEFAULT 'show',
                order_index INTEGER NOT NULL DEFAULT 0,
                metadata    TEXT NOT NULL DEFAULT '{}'
            );

            CREATE TABLE IF NOT EXISTS bumpers (
                id          TEXT PRIMARY KEY,
                channel_id  TEXT NOT NULL REFERENCES channels(id) ON DELETE CASCADE,
                title       TEXT NOT NULL DEFAULT '',
                file_path   TEXT NOT NULL DEFAULT '',
                torrent_hash TEXT NOT NULL DEFAULT '',
                duration    REAL NOT NULL DEFAULT 0,
                bumper_type TEXT NOT NULL DEFAULT 'transition',
                created_at  TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS commercials (
                id                  TEXT PRIMARY KEY,
                channel_id          TEXT NOT NULL REFERENCES channels(id) ON DELETE CASCADE,
                title               TEXT NOT NULL DEFAULT '',
                file_path           TEXT NOT NULL DEFAULT '',
                torrent_hash        TEXT NOT NULL DEFAULT '',
                duration            REAL NOT NULL DEFAULT 0,
                break_interval_sec  REAL NOT NULL DEFAULT 1800,
                created_at          TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS overlays (
                id          TEXT PRIMARY KEY,
                channel_id  TEXT NOT NULL REFERENCES channels(id) ON DELETE CASCADE,
                overlay_type TEXT NOT NULL DEFAULT 'text',
                content     TEXT NOT NULL DEFAULT '',
                position    TEXT NOT NULL DEFAULT 'bottom-left',
                start_offset REAL NOT NULL DEFAULT 0,
                end_offset   REAL NOT NULL DEFAULT 0,
                created_at  TEXT NOT NULL
            );
        """)
