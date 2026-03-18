"""
StreamStation42 - Channel and Show management.

Provides CRUD operations for channels, shows, lineup items,
bumpers, commercials and overlays backed by the SQLite database.
"""

import uuid
from datetime import datetime, timezone
from typing import Any

from .database import get_connection


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _new_id() -> str:
    return str(uuid.uuid4())


# ---------------------------------------------------------------------------
# Channel CRUD
# ---------------------------------------------------------------------------

def create_channel(name: str, description: str = "", owner: str = "anonymous",
                   config: dict | None = None, db_path: str = None) -> dict:
    """Create a new channel and return it as a dict."""
    import json
    channel_id = _new_id()
    created_at = _now_iso()
    cfg = json.dumps(config or {})
    with get_connection(db_path) as conn:
        conn.execute(
            "INSERT INTO channels (id, name, description, owner, created_at, config) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (channel_id, name, description, owner, created_at, cfg)
        )
    return get_channel(channel_id, db_path=db_path)


def get_channel(channel_id: str, db_path: str = None) -> dict | None:
    import json
    with get_connection(db_path) as conn:
        row = conn.execute(
            "SELECT * FROM channels WHERE id = ?", (channel_id,)
        ).fetchone()
    if row is None:
        return None
    d = dict(row)
    d["config"] = json.loads(d["config"])
    return d


def list_channels(db_path: str = None) -> list[dict]:
    import json
    with get_connection(db_path) as conn:
        rows = conn.execute("SELECT * FROM channels ORDER BY created_at").fetchall()
    result = []
    for row in rows:
        d = dict(row)
        d["config"] = json.loads(d["config"])
        result.append(d)
    return result


def update_channel(channel_id: str, name: str = None, description: str = None,
                   owner: str = None, config: dict = None, db_path: str = None) -> dict | None:
    import json
    existing = get_channel(channel_id, db_path=db_path)
    if existing is None:
        return None
    name = name if name is not None else existing["name"]
    description = description if description is not None else existing["description"]
    owner = owner if owner is not None else existing["owner"]
    cfg = json.dumps(config) if config is not None else json.dumps(existing["config"])
    with get_connection(db_path) as conn:
        conn.execute(
            "UPDATE channels SET name=?, description=?, owner=?, config=? WHERE id=?",
            (name, description, owner, cfg, channel_id)
        )
    return get_channel(channel_id, db_path=db_path)


def delete_channel(channel_id: str, db_path: str = None) -> bool:
    with get_connection(db_path) as conn:
        cur = conn.execute("DELETE FROM channels WHERE id=?", (channel_id,))
    return cur.rowcount > 0


# ---------------------------------------------------------------------------
# Show CRUD
# ---------------------------------------------------------------------------

def create_show(channel_id: str, title: str, description: str = "",
                duration: float = 0.0, file_path: str = "",
                torrent_hash: str = "", torrent_path: str = "",
                episode_number: int = 1, season_number: int = 1,
                db_path: str = None) -> dict:
    show_id = _new_id()
    created_at = _now_iso()
    with get_connection(db_path) as conn:
        conn.execute(
            "INSERT INTO shows (id, channel_id, title, description, duration, "
            "file_path, torrent_hash, torrent_path, episode_number, season_number, created_at) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            (show_id, channel_id, title, description, duration,
             file_path, torrent_hash, torrent_path,
             episode_number, season_number, created_at)
        )
    return get_show(show_id, db_path=db_path)


def get_show(show_id: str, db_path: str = None) -> dict | None:
    with get_connection(db_path) as conn:
        row = conn.execute("SELECT * FROM shows WHERE id=?", (show_id,)).fetchone()
    return dict(row) if row else None


def list_shows(channel_id: str, db_path: str = None) -> list[dict]:
    with get_connection(db_path) as conn:
        rows = conn.execute(
            "SELECT * FROM shows WHERE channel_id=? ORDER BY season_number, episode_number",
            (channel_id,)
        ).fetchall()
    return [dict(r) for r in rows]


def delete_show(show_id: str, db_path: str = None) -> bool:
    with get_connection(db_path) as conn:
        cur = conn.execute("DELETE FROM shows WHERE id=?", (show_id,))
    return cur.rowcount > 0


# ---------------------------------------------------------------------------
# Lineup CRUD
# ---------------------------------------------------------------------------

def add_lineup_item(channel_id: str, show_id: str | None = None,
                    item_type: str = "show", order_index: int = 0,
                    metadata: dict | None = None, db_path: str = None) -> dict:
    import json
    item_id = _new_id()
    meta = json.dumps(metadata or {})
    with get_connection(db_path) as conn:
        conn.execute(
            "INSERT INTO lineup_items (id, channel_id, show_id, item_type, order_index, metadata) "
            "VALUES (?,?,?,?,?,?)",
            (item_id, channel_id, show_id, item_type, order_index, meta)
        )
    return get_lineup_item(item_id, db_path=db_path)


def get_lineup_item(item_id: str, db_path: str = None) -> dict | None:
    import json
    with get_connection(db_path) as conn:
        row = conn.execute("SELECT * FROM lineup_items WHERE id=?", (item_id,)).fetchone()
    if row is None:
        return None
    d = dict(row)
    d["metadata"] = json.loads(d["metadata"])
    return d


def get_lineup(channel_id: str, db_path: str = None) -> list[dict]:
    """Return ordered lineup for a channel, enriched with show details."""
    import json
    with get_connection(db_path) as conn:
        rows = conn.execute(
            "SELECT li.*, s.title as show_title, s.duration as show_duration, "
            "s.torrent_hash as show_torrent_hash, s.file_path as show_file_path "
            "FROM lineup_items li "
            "LEFT JOIN shows s ON s.id = li.show_id "
            "WHERE li.channel_id = ? ORDER BY li.order_index",
            (channel_id,)
        ).fetchall()
    result = []
    for row in rows:
        d = dict(row)
        d["metadata"] = json.loads(d["metadata"])
        result.append(d)
    return result


def delete_lineup_item(item_id: str, db_path: str = None) -> bool:
    with get_connection(db_path) as conn:
        cur = conn.execute("DELETE FROM lineup_items WHERE id=?", (item_id,))
    return cur.rowcount > 0


def reorder_lineup(channel_id: str, ordered_ids: list[str], db_path: str = None) -> None:
    """Re-index lineup items according to the supplied ordered list of IDs."""
    with get_connection(db_path) as conn:
        for idx, item_id in enumerate(ordered_ids):
            conn.execute(
                "UPDATE lineup_items SET order_index=? WHERE id=? AND channel_id=?",
                (idx, item_id, channel_id)
            )


# ---------------------------------------------------------------------------
# Bumpers CRUD
# ---------------------------------------------------------------------------

def create_bumper(channel_id: str, title: str = "", file_path: str = "",
                  torrent_hash: str = "", duration: float = 0.0,
                  bumper_type: str = "transition", db_path: str = None) -> dict:
    bumper_id = _new_id()
    created_at = _now_iso()
    with get_connection(db_path) as conn:
        conn.execute(
            "INSERT INTO bumpers (id, channel_id, title, file_path, torrent_hash, "
            "duration, bumper_type, created_at) VALUES (?,?,?,?,?,?,?,?)",
            (bumper_id, channel_id, title, file_path, torrent_hash,
             duration, bumper_type, created_at)
        )
    return get_bumper(bumper_id, db_path=db_path)


def get_bumper(bumper_id: str, db_path: str = None) -> dict | None:
    with get_connection(db_path) as conn:
        row = conn.execute("SELECT * FROM bumpers WHERE id=?", (bumper_id,)).fetchone()
    return dict(row) if row else None


def list_bumpers(channel_id: str, db_path: str = None) -> list[dict]:
    with get_connection(db_path) as conn:
        rows = conn.execute(
            "SELECT * FROM bumpers WHERE channel_id=? ORDER BY created_at",
            (channel_id,)
        ).fetchall()
    return [dict(r) for r in rows]


def delete_bumper(bumper_id: str, db_path: str = None) -> bool:
    with get_connection(db_path) as conn:
        cur = conn.execute("DELETE FROM bumpers WHERE id=?", (bumper_id,))
    return cur.rowcount > 0


# ---------------------------------------------------------------------------
# Commercials CRUD
# ---------------------------------------------------------------------------

def create_commercial(channel_id: str, title: str = "", file_path: str = "",
                      torrent_hash: str = "", duration: float = 0.0,
                      break_interval_sec: float = 1800.0, db_path: str = None) -> dict:
    comm_id = _new_id()
    created_at = _now_iso()
    with get_connection(db_path) as conn:
        conn.execute(
            "INSERT INTO commercials (id, channel_id, title, file_path, torrent_hash, "
            "duration, break_interval_sec, created_at) VALUES (?,?,?,?,?,?,?,?)",
            (comm_id, channel_id, title, file_path, torrent_hash,
             duration, break_interval_sec, created_at)
        )
    return get_commercial(comm_id, db_path=db_path)


def get_commercial(comm_id: str, db_path: str = None) -> dict | None:
    with get_connection(db_path) as conn:
        row = conn.execute("SELECT * FROM commercials WHERE id=?", (comm_id,)).fetchone()
    return dict(row) if row else None


def list_commercials(channel_id: str, db_path: str = None) -> list[dict]:
    with get_connection(db_path) as conn:
        rows = conn.execute(
            "SELECT * FROM commercials WHERE channel_id=? ORDER BY created_at",
            (channel_id,)
        ).fetchall()
    return [dict(r) for r in rows]


def delete_commercial(comm_id: str, db_path: str = None) -> bool:
    with get_connection(db_path) as conn:
        cur = conn.execute("DELETE FROM commercials WHERE id=?", (comm_id,))
    return cur.rowcount > 0


# ---------------------------------------------------------------------------
# Overlays CRUD
# ---------------------------------------------------------------------------

def create_overlay(channel_id: str, overlay_type: str = "text",
                   content: str = "", position: str = "bottom-left",
                   start_offset: float = 0.0, end_offset: float = 0.0,
                   db_path: str = None) -> dict:
    ov_id = _new_id()
    created_at = _now_iso()
    with get_connection(db_path) as conn:
        conn.execute(
            "INSERT INTO overlays (id, channel_id, overlay_type, content, position, "
            "start_offset, end_offset, created_at) VALUES (?,?,?,?,?,?,?,?)",
            (ov_id, channel_id, overlay_type, content, position,
             start_offset, end_offset, created_at)
        )
    return get_overlay(ov_id, db_path=db_path)


def get_overlay(overlay_id: str, db_path: str = None) -> dict | None:
    with get_connection(db_path) as conn:
        row = conn.execute("SELECT * FROM overlays WHERE id=?", (overlay_id,)).fetchone()
    return dict(row) if row else None


def list_overlays(channel_id: str, db_path: str = None) -> list[dict]:
    with get_connection(db_path) as conn:
        rows = conn.execute(
            "SELECT * FROM overlays WHERE channel_id=? ORDER BY created_at",
            (channel_id,)
        ).fetchall()
    return [dict(r) for r in rows]


def delete_overlay(overlay_id: str, db_path: str = None) -> bool:
    with get_connection(db_path) as conn:
        cur = conn.execute("DELETE FROM overlays WHERE id=?", (overlay_id,))
    return cur.rowcount > 0
