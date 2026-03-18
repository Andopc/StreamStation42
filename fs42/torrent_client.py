"""
StreamStation42 - P2P Torrent Client

Handles BitTorrent-based content distribution for indie cable channels.
This module enables:
  - Seeding local AV1 content to other peers
  - Downloading channel content from magnets/info-hashes
  - Serving downloaded files to the MPV player
  - Providing peer-count and transfer statistics

libtorrent (python-libtorrent) is used when available; the module
degrades gracefully to a pure-Python fallback that:
  - Still builds and saves valid .torrent metainfo files
  - Generates correct magnet links for sharing
  - Plays any already-cached local files
"""

from __future__ import annotations

import base64
import hashlib
import json
import logging
import os
import struct
import time
import urllib.parse
from pathlib import Path
from threading import Lock, Thread
from typing import Any

logger = logging.getLogger("TorrentClient")

# ---------------------------------------------------------------------------
# Optional libtorrent import
# ---------------------------------------------------------------------------
try:
    import libtorrent as lt  # type: ignore
    _LT_AVAILABLE = True
    logger.info("libtorrent available — full P2P seeding enabled")
except ImportError:
    lt = None
    _LT_AVAILABLE = False
    logger.info("libtorrent not found — using metadata-only mode (local file playback + .torrent generation)")


# ---------------------------------------------------------------------------
# Public trackers used for peer discovery
# ---------------------------------------------------------------------------
DEFAULT_TRACKERS = [
    "udp://tracker.opentrackr.org:1337/announce",
    "udp://open.demonii.com:1337/announce",
    "udp://tracker.openbittorrent.com:80/announce",
    "wss://tracker.btorrent.xyz",
]


# ---------------------------------------------------------------------------
# Pure-Python bencode helpers (used for .torrent generation without libtorrent)
# ---------------------------------------------------------------------------

def _bencode(value: Any) -> bytes:
    if isinstance(value, bytes):
        return str(len(value)).encode() + b":" + value
    if isinstance(value, str):
        enc = value.encode("utf-8")
        return str(len(enc)).encode() + b":" + enc
    if isinstance(value, int):
        return b"i" + str(value).encode() + b"e"
    if isinstance(value, list):
        return b"l" + b"".join(_bencode(i) for i in value) + b"e"
    if isinstance(value, dict):
        body = b""
        for k in sorted(value.keys()):
            body += _bencode(k) + _bencode(value[k])
        return b"d" + body + b"e"
    raise TypeError(f"Cannot bencode type: {type(value)}")


def _piece_hashes(file_path: str, piece_length: int = 262144) -> tuple[bytes, int]:
    """Return (concatenated SHA-1 piece hashes, file_size)."""
    pieces = bytearray()
    size = 0
    with open(file_path, "rb") as fh:
        while True:
            chunk = fh.read(piece_length)
            if not chunk:
                break
            pieces += hashlib.sha1(chunk).digest()  # noqa: S324 – required by BT spec
            size += len(chunk)
    return bytes(pieces), size


def _build_info_dict(file_path: str, piece_length: int = 262144) -> dict:
    pieces, size = _piece_hashes(file_path, piece_length)
    return {
        "name": Path(file_path).name,
        "piece length": piece_length,
        "pieces": pieces,
        "length": size,
    }


def _info_hash_hex(info: dict) -> str:
    return hashlib.sha1(_bencode(info)).hexdigest()  # noqa: S324


def _magnet_link(info_hash: str, name: str,
                 trackers: list[str] | None = None) -> str:
    link = f"magnet:?xt=urn:btih:{info_hash}&dn={urllib.parse.quote(name)}"
    for t in (trackers or DEFAULT_TRACKERS):
        link += f"&tr={urllib.parse.quote(t, safe='')}"
    return link


def _extract_infohash_from_magnet(magnet: str) -> str | None:
    """Parse the btih info-hash out of a magnet link (40-char hex or 32-char base32)."""
    for part in magnet.split("&"):
        if part.startswith("magnet:?xt=urn:btih:") or part.startswith("xt=urn:btih:"):
            ih = part.split(":")[-1].lower()
            if len(ih) == 32:
                # base32 → hex
                try:
                    ih = base64.b32decode(ih.upper()).hex()
                except Exception:
                    pass
            return ih if len(ih) == 40 else None
    return None


# ---------------------------------------------------------------------------
# TorrentClient
# ---------------------------------------------------------------------------

class TorrentClient:
    """
    Singleton torrent client for StreamStation42.

    Manages seeding and downloading of AV1 channel content.
    Thread-safe; safe to construct multiple times (returns the same state).
    """

    _instance: "TorrentClient | None" = None
    _lock = Lock()

    # Borg-singleton: all instances share the same __dict__
    _shared_state: dict = {}

    def __new__(cls, torrent_dir: str = "torrents", listen_port: int = 6881):
        with cls._lock:
            if cls._instance is None:
                obj = super().__new__(cls)
                obj.__dict__ = cls._shared_state
                obj._init(torrent_dir, listen_port)
                cls._instance = obj
        return cls._instance

    def _init(self, torrent_dir: str, listen_port: int) -> None:
        self.torrent_dir = Path(torrent_dir)
        self.torrent_dir.mkdir(parents=True, exist_ok=True)
        self.listen_port = listen_port
        self._handles: dict[str, Any] = {}     # info_hash → handle or path
        self._meta: dict[str, dict] = {}       # info_hash → metadata dict
        self._session = None
        self._initialized = True

        if _LT_AVAILABLE:
            self._session = lt.session()
            self._session.listen_on(listen_port, listen_port + 10)

    # ------------------------------------------------------------------
    # Seeding
    # ------------------------------------------------------------------

    def seed_file(self, file_path: str) -> dict:
        """
        Register a local AV1 file for seeding to peers.

        Creates a .torrent metainfo file in ``torrent_dir`` and starts
        seeding if libtorrent is available.

        Returns a dict with ``info_hash``, ``torrent_path``, and ``magnet``.
        """
        path = Path(file_path)
        if not path.exists():
            raise FileNotFoundError(f"Content file not found: {file_path}")

        if _LT_AVAILABLE:
            return self._seed_with_lt(path)
        return self._seed_manual(path)

    def _seed_with_lt(self, path: Path) -> dict:
        fs = lt.file_storage()
        lt.add_files(fs, str(path))
        ct = lt.create_torrent(fs)
        ct.set_creator("StreamStation42")
        for t in DEFAULT_TRACKERS:
            ct.add_tracker(t)
        lt.set_piece_hashes(ct, str(path.parent))
        torrent_data = lt.bencode(ct.generate())
        ti = lt.torrent_info(lt.bdecode(torrent_data))
        ih = str(ti.info_hash())

        tfile = self.torrent_dir / f"{ih}.torrent"
        tfile.write_bytes(torrent_data)

        if ih not in self._handles:
            params = {"ti": ti, "save_path": str(path.parent)}
            self._handles[ih] = self._session.add_torrent(params)

        result = {
            "info_hash": ih,
            "torrent_path": str(tfile),
            "magnet": _magnet_link(ih, path.name),
        }
        self._meta[ih] = result
        return result

    def _seed_manual(self, path: Path) -> dict:
        info = _build_info_dict(str(path))
        ih = _info_hash_hex(info)

        metainfo = {
            "info": info,
            "announce": DEFAULT_TRACKERS[0],
            "announce-list": [[t] for t in DEFAULT_TRACKERS],
            "created by": "StreamStation42",
            "creation date": int(time.time()),
        }
        tfile = self.torrent_dir / f"{ih}.torrent"
        tfile.write_bytes(_bencode(metainfo))

        self._handles[ih] = str(path)
        result = {
            "info_hash": ih,
            "torrent_path": str(tfile),
            "magnet": _magnet_link(ih, path.name),
        }
        self._meta[ih] = result
        logger.info("Metadata-only seed created for %s (info_hash=%s)", path.name, ih)
        return result

    # ------------------------------------------------------------------
    # Downloading / resolving
    # ------------------------------------------------------------------

    def resolve_stream(self, stream_config: dict,
                       content_dir: str = "catalog") -> str | None:
        """
        Resolve a torrent stream entry to a playable local file path.

        Resolution order:
        1. ``file_path`` key in stream_config — use directly if file exists.
        2. Cached file in ``content_dir`` matching the info-hash.
        3. Download via libtorrent from the provided ``magnet`` link.
        4. Return None if content cannot be resolved.

        Args:
            stream_config: A dict with (optionally) ``file_path``, ``magnet``,
                           ``title``, ``duration``.
            content_dir:   Directory to look for and download content into.
        Returns:
            Absolute path to the local file, or None.
        """
        # 1. Explicit file_path provided
        fp = stream_config.get("file_path")
        if fp and os.path.exists(fp):
            logger.debug("resolve_stream: using explicit file_path=%s", fp)
            return fp

        magnet = stream_config.get("magnet", "")
        info_hash = _extract_infohash_from_magnet(magnet) if magnet else None

        # 2. Look for cached file matching info-hash or title
        content_path = Path(content_dir)
        if info_hash:
            cached = self._find_cached(info_hash, content_path)
            if cached:
                logger.debug("resolve_stream: found cached file at %s", cached)
                return cached

        # 3. Download via libtorrent
        if magnet and _LT_AVAILABLE:
            downloaded = self._download_magnet(magnet, content_dir,
                                               stream_config.get("title", "unknown"))
            if downloaded:
                return downloaded

        logger.warning(
            "resolve_stream: could not resolve stream '%s' — "
            "content not cached and libtorrent not available or download failed. "
            "Place the AV1 file in '%s' or install libtorrent.",
            stream_config.get("title", "?"),
            content_dir,
        )
        return None

    def _find_cached(self, info_hash: str, content_dir: Path) -> str | None:
        """Look for a file whose name contains the info-hash or whose .torrent matches."""
        if not content_dir.exists():
            return None
        # Check .torrent marker files
        marker = self.torrent_dir / f"{info_hash}.torrent"
        if marker.exists():
            meta = self._meta.get(info_hash)
            if meta and meta.get("file_path"):
                fp = meta["file_path"]
                if os.path.exists(fp):
                    return fp
        # Scan content_dir for media files named with the hash
        for ext in (".av1", ".mkv", ".mp4", ".webm", ".avi", ".mov"):
            for f in content_dir.rglob(f"*{info_hash}*{ext}"):
                return str(f)
        return None

    def _download_magnet(self, magnet: str, save_dir: str,
                         title: str = "content",
                         timeout: int = 300) -> str | None:
        """
        Download content from a magnet link using libtorrent.

        Blocks until the download is complete or ``timeout`` seconds elapse.
        Returns the path to the downloaded file, or None on failure.
        """
        if not _LT_AVAILABLE:
            return None

        save_path = Path(save_dir)
        save_path.mkdir(parents=True, exist_ok=True)

        params = lt.parse_magnet_uri(magnet)
        params.save_path = str(save_path)
        handle = self._session.add_torrent(params)

        logger.info("Downloading '%s' from magnet — waiting for metadata…", title)
        deadline = time.time() + timeout
        while not handle.has_metadata() and time.time() < deadline:
            time.sleep(1)

        if not handle.has_metadata():
            logger.error("Metadata timeout for '%s'", title)
            self._session.remove_torrent(handle)
            return None

        ti = handle.get_torrent_info()
        ih = str(ti.info_hash())
        self._handles[ih] = handle
        logger.info("Metadata received for '%s' (hash=%s) — downloading…", title, ih)

        # Wait for download to complete
        while not handle.status().is_seeding and time.time() < deadline:
            s = handle.status()
            logger.info("  Downloading %s: %.1f%% peers=%d",
                        title, s.progress * 100, s.num_peers)
            time.sleep(5)

        if not handle.status().is_seeding:
            logger.warning("Download incomplete for '%s' after %ds", title, timeout)
            return None

        # Find the largest file in the torrent (assumes one-file or we want the biggest)
        for i in range(ti.num_files()):
            f = ti.files().file_path(i)
            full = save_path / f
            if full.exists():
                result = {
                    "info_hash": ih,
                    "torrent_path": "",
                    "file_path": str(full),
                    "magnet": magnet,
                }
                self._meta[ih] = result
                logger.info("Download complete for '%s' → %s", title, full)
                return str(full)
        return None

    # ------------------------------------------------------------------
    # Status / listing
    # ------------------------------------------------------------------

    def list_active(self) -> list[dict]:
        """Return metadata about all active torrents (seeding or downloading)."""
        result = []
        if _LT_AVAILABLE and self._session:
            for ih, handle in list(self._handles.items()):
                if hasattr(handle, "is_valid") and not handle.is_valid():
                    continue
                if hasattr(handle, "status"):
                    s = handle.status()
                    result.append({
                        "info_hash": ih,
                        "name": handle.name(),
                        "num_peers": s.num_peers,
                        "num_seeds": s.num_seeds,
                        "upload_rate_kbs": round(s.upload_rate / 1024, 1),
                        "download_rate_kbs": round(s.download_rate / 1024, 1),
                        "total_uploaded_mb": round(s.total_upload / (1024 * 1024), 2),
                        "progress": round(s.progress * 100, 1),
                        "state": str(s.state),
                        "magnet": self._meta.get(ih, {}).get("magnet", ""),
                    })
        else:
            for ih, handle in self._handles.items():
                meta = self._meta.get(ih, {})
                result.append({
                    "info_hash": ih,
                    "name": meta.get("name", ih),
                    "num_peers": 0,
                    "num_seeds": 0,
                    "upload_rate_kbs": 0,
                    "download_rate_kbs": 0,
                    "total_uploaded_mb": 0,
                    "progress": 100.0,
                    "state": "seeding (metadata-only)",
                    "magnet": meta.get("magnet", ""),
                })
        return result

    def remove(self, info_hash: str) -> bool:
        """Stop seeding/downloading a torrent by info-hash."""
        if info_hash not in self._handles:
            return False
        if _LT_AVAILABLE and self._session:
            handle = self._handles[info_hash]
            if hasattr(handle, "is_valid") and handle.is_valid():
                self._session.remove_torrent(handle)
        del self._handles[info_hash]
        self._meta.pop(info_hash, None)
        return True

    def get_magnet(self, info_hash: str) -> str:
        """Return the magnet link for a registered info-hash."""
        return self._meta.get(info_hash, {}).get("magnet", "")

    @property
    def libtorrent_available(self) -> bool:
        return _LT_AVAILABLE


# ---------------------------------------------------------------------------
# Module-level helpers
# ---------------------------------------------------------------------------

def get_client(torrent_dir: str = "torrents",
               listen_port: int = 6881) -> TorrentClient:
    """Return (or create) the module-level TorrentClient singleton."""
    return TorrentClient(torrent_dir=torrent_dir, listen_port=listen_port)
