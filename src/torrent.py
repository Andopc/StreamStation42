"""
StreamStation42 - BitTorrent / P2P layer.

Handles:
  - Creating .torrent metainfo files for AV1 show/bumper/commercial files.
  - Starting a seeder session so the host node seeds content to peers.
  - Providing magnet links for client-side WebTorrent / webtorrent-hybrid.
  - Listing active torrents and their peer counts.

libtorrent is used when available; when it is absent the module degrades
gracefully, storing torrent metadata in the database as base64-encoded
torrent dictionaries and returning stub handles.
"""

from __future__ import annotations

import base64
import hashlib
import json
import os
import struct
import time
from pathlib import Path
from typing import Any

# ---------------------------------------------------------------------------
# Optional libtorrent import
# ---------------------------------------------------------------------------
try:
    import libtorrent as lt  # type: ignore
    _LT_AVAILABLE = True
except ImportError:
    lt = None
    _LT_AVAILABLE = False


# ---------------------------------------------------------------------------
# Bencode helpers (used when libtorrent is unavailable)
# ---------------------------------------------------------------------------

def _bencode(value: Any) -> bytes:
    if isinstance(value, bytes):
        return str(len(value)).encode() + b":" + value
    if isinstance(value, str):
        encoded = value.encode("utf-8")
        return str(len(encoded)).encode() + b":" + encoded
    if isinstance(value, int):
        return b"i" + str(value).encode() + b"e"
    if isinstance(value, list):
        return b"l" + b"".join(_bencode(i) for i in value) + b"e"
    if isinstance(value, dict):
        encoded_items = b""
        for k in sorted(value.keys()):
            encoded_items += _bencode(k) + _bencode(value[k])
        return b"d" + encoded_items + b"e"
    raise TypeError(f"Cannot bencode type: {type(value)}")


def _make_torrent_info(file_path: str, piece_length: int = 262144) -> dict:
    """
    Build a minimal BitTorrent metainfo dict for a single file.

    Uses SHA-1 piece hashes (standard BT v1 format).
    """
    path = Path(file_path)
    if not path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")

    file_size = path.stat().st_size
    pieces = bytearray()
    with open(path, "rb") as fh:
        while True:
            chunk = fh.read(piece_length)
            if not chunk:
                break
            pieces += hashlib.sha1(chunk).digest()

    info = {
        "name": path.name,
        "piece length": piece_length,
        "pieces": bytes(pieces),
        "length": file_size,
    }
    return info


def _info_hash_from_info(info: dict) -> str:
    """Compute the 20-byte info-hash (hex) from an info dict."""
    return hashlib.sha1(_bencode(info)).hexdigest()


def _magnet_link(info_hash: str, name: str, trackers: list[str] | None = None) -> str:
    xt = f"urn:btih:{info_hash}"
    link = f"magnet:?xt={xt}&dn={name}"
    for t in (trackers or _DEFAULT_TRACKERS):
        import urllib.parse
        link += f"&tr={urllib.parse.quote(t, safe='')}"
    return link


_DEFAULT_TRACKERS = [
    "udp://tracker.opentrackr.org:1337/announce",
    "udp://open.demonii.com:1337/announce",
    "udp://tracker.openbittorrent.com:80/announce",
]


# ---------------------------------------------------------------------------
# High-level torrent manager
# ---------------------------------------------------------------------------

class TorrentManager:
    """
    Manages seeding of show/bumper/commercial files for a StreamStation42 node.

    Each file is represented by a .torrent metainfo file saved in
    `torrent_dir`.  The info-hash is stored in the SQLite database alongside
    the file record so peers can find it.

    When libtorrent is available, torrents are actually added to a live
    session and seeded.  Otherwise the class operates in 'metadata-only' mode
    and returns pre-generated magnet links.
    """

    def __init__(self, torrent_dir: str = "torrents",
                 listen_port: int = 6881,
                 trackers: list[str] | None = None):
        self.torrent_dir = Path(torrent_dir)
        self.torrent_dir.mkdir(parents=True, exist_ok=True)
        self.trackers = trackers or _DEFAULT_TRACKERS
        self._session = None
        self._handles: dict[str, Any] = {}  # info_hash -> lt.torrent_handle

        if _LT_AVAILABLE:
            self._session = lt.session()
            self._session.listen_on(listen_port, listen_port + 10)
            settings = lt.session_settings()
            settings.connections_limit = 200
            self._session.set_settings(settings)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def add_file(self, file_path: str) -> dict:
        """
        Register a media file for seeding.

        Returns a dict with:
          - info_hash (str): 40-char hex info-hash
          - torrent_path (str): path to the saved .torrent file
          - magnet (str): magnet link
        """
        path = Path(file_path)
        if not path.exists():
            raise FileNotFoundError(f"Media file not found: {file_path}")

        if _LT_AVAILABLE:
            return self._add_file_lt(path)
        return self._add_file_manual(path)

    def remove_file(self, info_hash: str) -> None:
        """Stop seeding a torrent identified by its info-hash."""
        if _LT_AVAILABLE and info_hash in self._handles:
            self._session.remove_torrent(self._handles.pop(info_hash))

    def get_magnet(self, info_hash: str, name: str = "") -> str:
        """Return a magnet link for an already-added torrent."""
        return _magnet_link(info_hash, name, self.trackers)

    def list_active(self) -> list[dict]:
        """
        Return info about all actively seeded torrents.

        Each dict contains: info_hash, name, num_peers, upload_rate,
        total_uploaded, state.
        """
        if not _LT_AVAILABLE or self._session is None:
            return [
                {"info_hash": ih, "name": "", "num_peers": 0,
                 "upload_rate": 0, "total_uploaded": 0, "state": "metadata-only"}
                for ih in self._handles
            ]
        result = []
        for ih, handle in list(self._handles.items()):
            if not handle.is_valid():
                continue
            status = handle.status()
            result.append({
                "info_hash": ih,
                "name": handle.name(),
                "num_peers": status.num_peers,
                "upload_rate": status.upload_rate,
                "total_uploaded": status.total_upload,
                "state": str(status.state),
            })
        return result

    def seed_from_torrent_file(self, torrent_path: str, save_path: str) -> str:
        """
        Start seeding from an existing .torrent file.  Useful when a peer
        joins the network and wants to help seed content it has downloaded.

        Returns the info-hash hex string.
        """
        if not _LT_AVAILABLE:
            info = lt.torrent_info(torrent_path) if lt else None
            return ""
        ti = lt.torrent_info(torrent_path)
        ih = str(ti.info_hash())
        if ih not in self._handles:
            params = {"ti": ti, "save_path": save_path}
            handle = self._session.add_torrent(params)
            self._handles[ih] = handle
        return ih

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _add_file_lt(self, path: Path) -> dict:
        """Add a file using native libtorrent."""
        fs = lt.file_storage()
        lt.add_files(fs, str(path))
        ct = lt.create_torrent(fs)
        ct.set_creator("StreamStation42")
        for t in self.trackers:
            ct.add_tracker(t)
        lt.set_piece_hashes(ct, str(path.parent))
        torrent_data = lt.bencode(ct.generate())
        ti = lt.torrent_info(lt.bdecode(torrent_data))
        info_hash = str(ti.info_hash())

        torrent_file = self.torrent_dir / f"{info_hash}.torrent"
        torrent_file.write_bytes(torrent_data)

        if info_hash not in self._handles:
            params = {"ti": ti, "save_path": str(path.parent)}
            handle = self._session.add_torrent(params)
            self._handles[info_hash] = handle

        return {
            "info_hash": info_hash,
            "torrent_path": str(torrent_file),
            "magnet": _magnet_link(info_hash, path.name, self.trackers),
        }

    def _add_file_manual(self, path: Path) -> dict:
        """Build a .torrent file without libtorrent (pure-Python fallback)."""
        info = _make_torrent_info(str(path))
        info_hash = _info_hash_from_info(info)

        metainfo = {
            "info": info,
            "announce": self.trackers[0] if self.trackers else "",
            "announce-list": [[t] for t in self.trackers],
            "created by": "StreamStation42",
            "creation date": int(time.time()),
        }

        torrent_file = self.torrent_dir / f"{info_hash}.torrent"
        torrent_file.write_bytes(_bencode(metainfo))

        # Store the handle as just the info_hash string in metadata-only mode
        self._handles[info_hash] = info_hash

        return {
            "info_hash": info_hash,
            "torrent_path": str(torrent_file),
            "magnet": _magnet_link(info_hash, path.name, self.trackers),
        }


# ---------------------------------------------------------------------------
# Module-level singleton (created lazily by the server)
# ---------------------------------------------------------------------------

_manager: TorrentManager | None = None


def get_manager(torrent_dir: str = "torrents",
                listen_port: int = 6881) -> TorrentManager:
    global _manager
    if _manager is None:
        _manager = TorrentManager(torrent_dir=torrent_dir,
                                  listen_port=listen_port)
    return _manager
