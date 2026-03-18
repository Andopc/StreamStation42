"""
StreamStation42 - P2P Torrent REST API

Provides endpoints for managing the BitTorrent P2P layer:
  GET  /torrent/status        — list active torrents and peer counts
  POST /torrent/seed          — start seeding a local file (returns magnet)
  POST /torrent/add           — add a magnet link for downloading/seeding
  DELETE /torrent/{info_hash} — remove a torrent from the active session

These endpoints integrate with the existing FieldStation42 FastAPI server.
"""

import logging
import os

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from fs42.torrent_client import get_client, _LT_AVAILABLE

router = APIRouter(prefix="/torrent", tags=["P2P Torrent"])
logger = logging.getLogger("TorrentAPI")


# ---------------------------------------------------------------------------
# Request/response models
# ---------------------------------------------------------------------------

class SeedRequest(BaseModel):
    file_path: str


class AddMagnetRequest(BaseModel):
    magnet: str
    title: str = "Unknown"
    save_dir: str = "catalog"


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.get("/status")
def get_torrent_status():
    """
    Return the status of all active torrents, including peer counts,
    upload/download rates, and magnet links.
    """
    client = get_client()
    torrents = client.list_active()
    return {
        "libtorrent_available": _LT_AVAILABLE,
        "active_count": len(torrents),
        "torrents": torrents,
    }


@router.post("/seed")
def seed_file(req: SeedRequest):
    """
    Start seeding a local AV1 (or other video) file.

    Returns the info-hash, .torrent file path, and magnet link so the
    channel host can share content with peers.
    """
    if not os.path.exists(req.file_path):
        raise HTTPException(
            status_code=404,
            detail=f"File not found: {req.file_path}",
        )

    client = get_client()
    try:
        result = client.seed_file(req.file_path)
        logger.info("Started seeding %s → %s", req.file_path, result["magnet"])
        return {
            "status": "seeding",
            "info_hash": result["info_hash"],
            "torrent_path": result["torrent_path"],
            "magnet": result["magnet"],
            "libtorrent_available": _LT_AVAILABLE,
        }
    except Exception as exc:
        logger.exception("Error seeding %s", req.file_path)
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/add")
def add_magnet(req: AddMagnetRequest):
    """
    Add a magnet link to the torrent client for downloading and seeding.

    If libtorrent is available the content will be downloaded into
    ``save_dir`` and then seeded to other peers.  If libtorrent is not
    available the request is acknowledged but no download occurs — content
    must be placed manually.
    """
    client = get_client()
    if not _LT_AVAILABLE:
        return {
            "status": "acknowledged",
            "message": (
                "libtorrent is not installed — content cannot be downloaded "
                "automatically.  Place the file in the configured torrent_dir "
                "and restart the player."
            ),
            "libtorrent_available": False,
        }

    try:
        local_path = client._download_magnet(req.magnet, req.save_dir, req.title)
        if local_path:
            return {
                "status": "downloaded",
                "file_path": local_path,
                "libtorrent_available": True,
            }
        raise HTTPException(
            status_code=504,
            detail="Download timed out or failed — check the magnet link and network connectivity.",
        )
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("Error adding magnet %s", req.magnet)
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.delete("/{info_hash}")
def remove_torrent(info_hash: str):
    """Remove a torrent from the active session by its 40-char hex info-hash."""
    client = get_client()
    removed = client.remove(info_hash)
    if not removed:
        raise HTTPException(
            status_code=404,
            detail=f"Torrent with info-hash '{info_hash}' not found in active session.",
        )
    return {"status": "removed", "info_hash": info_hash}
