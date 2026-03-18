"""
StreamStation42 - Schedule Builder API

Provides endpoints for the visual schedule builder GUI:

  GET  /schedule-builder/stations
       List all configured stations (name, type, channel number).

  GET  /schedule-builder/{network_name}
       Return the full weekly schedule for a station in a normalised form
       the GUI can consume directly.

  PUT  /schedule-builder/{network_name}
       Accept a modified schedule payload and write it back into the station's
       JSON configuration file.

  GET  /schedule-builder/browse
       Browse the local filesystem (restricted to the project root) returning
       sub-directories and video files — used by the file-picker in the GUI.

Both "standard" (tag/folder-based) and "torrent" (magnet/file-based) channel
types are fully supported.
"""

from __future__ import annotations

import json
import logging
import os
from typing import Any

from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel

from fs42.station_io import StationIO
from fs42.station_manager import StationManager

router = APIRouter(prefix="/schedule-builder", tags=["Schedule Builder"])
logger = logging.getLogger("ScheduleBuilder")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DAYS = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]
HOURS = list(range(24))

VIDEO_EXTENSIONS = {
    ".av1", ".mp4", ".mkv", ".webm", ".avi", ".mov", ".m4v", ".ts", ".flv",
    ".ogv", ".wmv", ".mpg", ".mpeg",
}

# Project root: this file lives at fs42/fs42_server/api/schedule_builder.py
_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(_THIS_DIR, "..", "..", ".."))


# ---------------------------------------------------------------------------
# Path helpers
# ---------------------------------------------------------------------------

def _safe_resolve(relative_path: str) -> str:
    """Resolve *relative_path* against PROJECT_ROOT and reject escapes."""
    clean = relative_path.lstrip("/").lstrip("\\")
    resolved = os.path.realpath(os.path.join(PROJECT_ROOT, clean))
    if not resolved.startswith(PROJECT_ROOT):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Path traversal detected — path must stay within the project directory.",
        )
    return resolved


# ---------------------------------------------------------------------------
# Schedule normalisation helpers
# ---------------------------------------------------------------------------

def _normalize_standard_schedule(raw_conf: dict) -> dict:
    """
    Extract the weekly schedule from a raw station config dict and return
    it as a plain dict keyed by day → hour → slot_config.
    """
    schedule: dict[str, dict[str, Any]] = {}
    day_templates = raw_conf.get("day_templates", {})

    for day in DAYS:
        day_val = raw_conf.get(day, {})
        # Day value may be a string referencing a template
        if isinstance(day_val, str):
            day_val = day_templates.get(day_val, {})
        schedule[day] = {str(h): day_val.get(str(h), {}) for h in HOURS}

    return schedule


def _normalize_torrent_schedule(raw_conf: dict) -> list[dict]:
    """Return the torrent_streams list (or an empty list)."""
    return raw_conf.get("torrent_streams", [])


# ---------------------------------------------------------------------------
# Request / response models
# ---------------------------------------------------------------------------

class SlotConfig(BaseModel):
    """A single time-slot configuration for a standard channel."""
    tags: str | list[str] | None = None
    event: str | None = None           # e.g. "signoff"
    file_path: str | None = None       # direct file path override
    break_strategy: str | None = None
    bump_dir: str | None = None
    commercial_dir: str | None = None
    sequence: str | None = None
    label: str | None = None           # friendly display label (GUI only)


class WeeklySchedulePayload(BaseModel):
    """
    Payload sent by the GUI to update a standard channel's weekly schedule.
    Keys are day names; values are dicts of hour-string → SlotConfig.
    """
    schedule: dict[str, dict[str, Any]]


class TorrentStreamEntry(BaseModel):
    title: str
    duration: int
    magnet: str = ""
    file_path: str = ""
    description: str = ""


class TorrentSchedulePayload(BaseModel):
    torrent_streams: list[TorrentStreamEntry]
    torrent_dir: str = "catalog"


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.get("/stations")
def list_stations():
    """Return a compact list of all configured stations."""
    sio = StationIO()
    raw = sio.list_raw_station_configs()
    return [
        {
            "network_name": c.get("station_conf", {}).get("network_name", "?"),
            "network_type": c.get("station_conf", {}).get("network_type", "standard"),
            "channel_number": c.get("station_conf", {}).get("channel_number", 0),
        }
        for c in raw
        if "station_conf" in c
    ]


@router.get("/browse")
def browse_filesystem(path: str = ""):
    """
    Browse a directory inside the project root.

    Returns sub-directories and video files at the given *path*.
    *path* is relative to the project root (e.g. ``catalog`` or
    ``catalog/myshow``).
    """
    if not path:
        path = ""
    resolved = _safe_resolve(path)

    if not os.path.exists(resolved):
        # Return empty result rather than 404 so the GUI can handle gracefully
        return {"path": path, "parent": "", "dirs": [], "files": []}

    if not os.path.isdir(resolved):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Path is not a directory.",
        )

    dirs: list[dict] = []
    files: list[dict] = []

    try:
        for entry in sorted(os.scandir(resolved), key=lambda e: (e.is_file(), e.name.lower())):
            rel = os.path.relpath(os.path.join(resolved, entry.name), PROJECT_ROOT)
            if entry.is_dir():
                dirs.append({"name": entry.name, "path": rel.replace("\\", "/")})
            elif entry.is_file():
                ext = os.path.splitext(entry.name)[1].lower()
                if ext in VIDEO_EXTENSIONS:
                    files.append({
                        "name": entry.name,
                        "path": rel.replace("\\", "/"),
                        "size_mb": round(entry.stat().st_size / (1024 * 1024), 1),
                    })
    except PermissionError:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Permission denied reading directory.",
        )

    # Compute parent path
    parent = ""
    if path:
        parent_abs = os.path.dirname(resolved)
        if parent_abs.startswith(PROJECT_ROOT):
            parent = os.path.relpath(parent_abs, PROJECT_ROOT).replace("\\", "/")
            if parent == ".":
                parent = ""

    return {"path": path or "", "parent": parent, "dirs": dirs, "files": files}


@router.get("/{network_name}")
def get_schedule(network_name: str):
    """
    Return the schedule for *network_name* in a GUI-friendly normalised form.

    For **standard** channels this includes the full weekly slot grid.
    For **torrent** channels this returns the ``torrent_streams`` list.
    """
    sio = StationIO()
    ok, raw_data, err = sio.read_raw_station_config(network_name)
    if not ok:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=err)

    conf = raw_data.get("station_conf", {})
    ntype = conf.get("network_type", "standard")

    result: dict[str, Any] = {
        "network_name": conf.get("network_name"),
        "network_type": ntype,
        "channel_number": conf.get("channel_number"),
        "content_dir": conf.get("content_dir", "catalog"),
        "bump_dir": conf.get("bump_dir", ""),
        "commercial_dir": conf.get("commercial_dir", ""),
        "schedule_increment": conf.get("schedule_increment", 30),
        "break_strategy": conf.get("break_strategy", "standard"),
    }

    if ntype == "torrent":
        result["torrent_streams"] = _normalize_torrent_schedule(conf)
        result["torrent_dir"] = conf.get("torrent_dir", "catalog")
    else:
        result["schedule"] = _normalize_standard_schedule(conf)
        result["day_templates"] = conf.get("day_templates", {})

    return result


@router.put("/{network_name}/weekly")
def save_weekly_schedule(network_name: str, payload: WeeklySchedulePayload):
    """
    Save a modified weekly schedule for a **standard** channel.

    Only the per-day hour entries are updated; all other station config
    fields remain unchanged.
    """
    sio = StationIO()
    ok, raw_data, err = sio.read_raw_station_config(network_name)
    if not ok:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=err)

    conf = raw_data.get("station_conf", {})
    ntype = conf.get("network_type", "standard")

    if ntype == "torrent":
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Use PUT /{network_name}/torrent-streams for torrent channels.",
        )

    # Apply each day from the payload — keep only non-empty slots
    for day in DAYS:
        if day in payload.schedule:
            day_slots: dict[str, Any] = {}
            for hour_str, slot in payload.schedule[day].items():
                if not slot:
                    continue
                # Drop GUI-only fields before saving
                clean = {k: v for k, v in slot.items()
                         if v is not None and v != "" and k != "label"}
                if clean:
                    day_slots[hour_str] = clean
            conf[day] = day_slots

    raw_data["station_conf"] = conf

    # Write back
    mgr = StationManager()
    success, message, _fp = mgr.write_station_config(
        network_name, raw_data, is_update=True
    )
    if not success:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail=message
        )

    logger.info("Saved weekly schedule for station '%s'", network_name)
    return {"status": "saved", "network_name": network_name, "message": message}


@router.put("/{network_name}/torrent-streams")
def save_torrent_streams(network_name: str, payload: TorrentSchedulePayload):
    """
    Save a modified ``torrent_streams`` list for a **torrent** channel.

    Replaces the existing list and updates ``torrent_dir`` if provided.
    All other station config fields remain unchanged.
    """
    sio = StationIO()
    ok, raw_data, err = sio.read_raw_station_config(network_name)
    if not ok:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=err)

    conf = raw_data.get("station_conf", {})

    conf["torrent_streams"] = [s.model_dump(exclude_none=True) for s in payload.torrent_streams]
    if payload.torrent_dir:
        conf["torrent_dir"] = payload.torrent_dir

    raw_data["station_conf"] = conf

    mgr = StationManager()
    success, message, _fp = mgr.write_station_config(
        network_name, raw_data, is_update=True
    )
    if not success:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail=message
        )

    logger.info("Saved torrent streams for station '%s'", network_name)
    return {"status": "saved", "network_name": network_name, "message": message}
