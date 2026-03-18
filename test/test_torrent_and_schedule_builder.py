"""
Tests for fs42/torrent_client.py

These tests cover the pure-Python (no libtorrent) code paths so they run
without any optional C++ dependency.
"""

import hashlib
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

# Ensure project root is on sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import fs42.torrent_client as tc


# ---------------------------------------------------------------------------
# Helper: create a small dummy AV1-like file in a temp dir
# ---------------------------------------------------------------------------

def _make_dummy_file(directory: str, name: str = "ep1.av1", size: int = 1024) -> str:
    path = os.path.join(directory, name)
    with open(path, "wb") as fh:
        fh.write(os.urandom(size))
    return path


# ---------------------------------------------------------------------------
# bencode
# ---------------------------------------------------------------------------

class TestBencode(unittest.TestCase):

    def test_bencode_int(self):
        self.assertEqual(tc._bencode(42), b"i42e")

    def test_bencode_negative_int(self):
        self.assertEqual(tc._bencode(-1), b"i-1e")

    def test_bencode_bytes(self):
        self.assertEqual(tc._bencode(b"abc"), b"3:abc")

    def test_bencode_str(self):
        self.assertEqual(tc._bencode("hello"), b"5:hello")

    def test_bencode_list(self):
        self.assertEqual(tc._bencode([1, "a"]), b"li1e1:ae")

    def test_bencode_dict_sorted(self):
        result = tc._bencode({"b": 2, "a": 1})
        self.assertEqual(result, b"d1:ai1e1:bi2ee")

    def test_bencode_nested(self):
        data = {"info": {"length": 100, "name": "test"}}
        encoded = tc._bencode(data)
        self.assertIn(b"info", encoded)

    def test_bencode_unsupported_type_raises(self):
        with self.assertRaises(TypeError):
            tc._bencode(3.14)


# ---------------------------------------------------------------------------
# Magnet-link helpers
# ---------------------------------------------------------------------------

class TestMagnetHelpers(unittest.TestCase):

    def test_magnet_link_contains_btih(self):
        link = tc._magnet_link("abc123" * 6 + "abcd", "My Show")
        self.assertIn("xt=urn:btih:", link)
        self.assertIn("dn=My+Show", link.replace("%20", "+"))

    def test_extract_infohash_standard(self):
        ih = "a" * 40
        magnet = f"magnet:?xt=urn:btih:{ih}&dn=test"
        extracted = tc._extract_infohash_from_magnet(magnet)
        self.assertEqual(extracted, ih)

    def test_extract_infohash_none_on_bad_magnet(self):
        self.assertIsNone(tc._extract_infohash_from_magnet("not-a-magnet"))

    def test_extract_infohash_none_on_empty(self):
        self.assertIsNone(tc._extract_infohash_from_magnet(""))

    def test_extract_infohash_short_rejected(self):
        # Only 10 chars — not a valid info-hash
        magnet = "magnet:?xt=urn:btih:abc123defg"
        self.assertIsNone(tc._extract_infohash_from_magnet(magnet))


# ---------------------------------------------------------------------------
# Info-hash computation
# ---------------------------------------------------------------------------

class TestInfoHash(unittest.TestCase):

    def test_piece_hashes_returns_bytes_and_size(self):
        with tempfile.TemporaryDirectory() as d:
            path = _make_dummy_file(d, size=512)
            pieces, size = tc._piece_hashes(path)
            self.assertIsInstance(pieces, bytes)
            self.assertEqual(len(pieces), 20)   # one SHA-1 hash for ≤ piece_length
            self.assertEqual(size, 512)

    def test_build_info_dict_has_required_keys(self):
        with tempfile.TemporaryDirectory() as d:
            path = _make_dummy_file(d, size=256)
            info = tc._build_info_dict(path)
            self.assertIn("name", info)
            self.assertIn("piece length", info)
            self.assertIn("pieces", info)
            self.assertIn("length", info)
            self.assertEqual(info["name"], "ep1.av1")
            self.assertEqual(info["length"], 256)

    def test_info_hash_hex_is_40_chars(self):
        with tempfile.TemporaryDirectory() as d:
            path = _make_dummy_file(d, size=256)
            info = tc._build_info_dict(path)
            ih = tc._info_hash_hex(info)
            self.assertEqual(len(ih), 40)
            # Must be hex
            int(ih, 16)

    def test_same_file_same_hash(self):
        with tempfile.TemporaryDirectory() as d:
            path = _make_dummy_file(d, size=256)
            info1 = tc._build_info_dict(path)
            info2 = tc._build_info_dict(path)
            self.assertEqual(tc._info_hash_hex(info1), tc._info_hash_hex(info2))

    def test_different_files_different_hashes(self):
        with tempfile.TemporaryDirectory() as d:
            p1 = _make_dummy_file(d, "a.av1", 256)
            p2 = _make_dummy_file(d, "b.av1", 256)
            i1 = tc._build_info_dict(p1)
            i2 = tc._build_info_dict(p2)
            # Different file names → different info dicts → different hashes
            # (name is part of info dict)
            self.assertNotEqual(tc._info_hash_hex(i1), tc._info_hash_hex(i2))


# ---------------------------------------------------------------------------
# TorrentClient — metadata-only seeding (no libtorrent)
# ---------------------------------------------------------------------------

class TestTorrentClientMetadataOnly(unittest.TestCase):
    """Tests for the pure-Python / no-libtorrent code path."""

    def setUp(self):
        # Reset the singleton before each test so we get a fresh client
        tc.TorrentClient._instance = None
        tc.TorrentClient._shared_state = {}

    def test_seed_file_creates_torrent_file(self):
        with tempfile.TemporaryDirectory() as d:
            content_file = _make_dummy_file(d, size=512)
            torrent_dir = os.path.join(d, "torrents")

            with patch.object(tc, "_LT_AVAILABLE", False):
                tc.TorrentClient._instance = None
                tc.TorrentClient._shared_state = {}
                client = tc.TorrentClient(torrent_dir=torrent_dir)
                result = client.seed_file(content_file)

            self.assertIn("info_hash", result)
            self.assertIn("magnet", result)
            self.assertIn("torrent_path", result)
            self.assertTrue(os.path.exists(result["torrent_path"]))
            self.assertTrue(result["torrent_path"].endswith(".torrent"))

    def test_seed_file_missing_raises(self):
        with tempfile.TemporaryDirectory() as d:
            with patch.object(tc, "_LT_AVAILABLE", False):
                tc.TorrentClient._instance = None
                tc.TorrentClient._shared_state = {}
                client = tc.TorrentClient(torrent_dir=d)
                with self.assertRaises(FileNotFoundError):
                    client.seed_file("/nonexistent/path/nope.av1")

    def test_list_active_returns_list(self):
        with tempfile.TemporaryDirectory() as d:
            content_file = _make_dummy_file(d, size=256)
            with patch.object(tc, "_LT_AVAILABLE", False):
                tc.TorrentClient._instance = None
                tc.TorrentClient._shared_state = {}
                client = tc.TorrentClient(torrent_dir=d)
                client.seed_file(content_file)
                active = client.list_active()
            self.assertIsInstance(active, list)
            self.assertEqual(len(active), 1)
            self.assertIn("info_hash", active[0])
            self.assertIn("magnet", active[0])

    def test_remove_torrent(self):
        with tempfile.TemporaryDirectory() as d:
            content_file = _make_dummy_file(d, size=256)
            with patch.object(tc, "_LT_AVAILABLE", False):
                tc.TorrentClient._instance = None
                tc.TorrentClient._shared_state = {}
                client = tc.TorrentClient(torrent_dir=d)
                result = client.seed_file(content_file)
                ih = result["info_hash"]
                removed = client.remove(ih)
                self.assertTrue(removed)
                self.assertEqual(client.list_active(), [])

    def test_remove_nonexistent_returns_false(self):
        with tempfile.TemporaryDirectory() as d:
            with patch.object(tc, "_LT_AVAILABLE", False):
                tc.TorrentClient._instance = None
                tc.TorrentClient._shared_state = {}
                client = tc.TorrentClient(torrent_dir=d)
                self.assertFalse(client.remove("a" * 40))

    def test_get_magnet_after_seed(self):
        with tempfile.TemporaryDirectory() as d:
            content_file = _make_dummy_file(d, size=256)
            with patch.object(tc, "_LT_AVAILABLE", False):
                tc.TorrentClient._instance = None
                tc.TorrentClient._shared_state = {}
                client = tc.TorrentClient(torrent_dir=d)
                result = client.seed_file(content_file)
                magnet = client.get_magnet(result["info_hash"])
            self.assertIn("magnet:?xt=urn:btih:", magnet)


# ---------------------------------------------------------------------------
# TorrentClient.resolve_stream
# ---------------------------------------------------------------------------

class TestResolveStream(unittest.TestCase):

    def setUp(self):
        tc.TorrentClient._instance = None
        tc.TorrentClient._shared_state = {}

    def test_resolve_explicit_file_path(self):
        with tempfile.TemporaryDirectory() as d:
            fp = _make_dummy_file(d)
            with patch.object(tc, "_LT_AVAILABLE", False):
                tc.TorrentClient._instance = None
                tc.TorrentClient._shared_state = {}
                client = tc.TorrentClient(torrent_dir=d)
                result = client.resolve_stream({"file_path": fp, "title": "test", "duration": 60}, d)
            self.assertEqual(result, fp)

    def test_resolve_missing_explicit_path_returns_none(self):
        with tempfile.TemporaryDirectory() as d:
            with patch.object(tc, "_LT_AVAILABLE", False):
                tc.TorrentClient._instance = None
                tc.TorrentClient._shared_state = {}
                client = tc.TorrentClient(torrent_dir=d)
                result = client.resolve_stream(
                    {"file_path": "/nonexistent/ep.av1", "title": "x", "duration": 60}, d
                )
            # No file_path on disk and no magnet → None
            self.assertIsNone(result)

    def test_resolve_no_info_returns_none(self):
        with tempfile.TemporaryDirectory() as d:
            with patch.object(tc, "_LT_AVAILABLE", False):
                tc.TorrentClient._instance = None
                tc.TorrentClient._shared_state = {}
                client = tc.TorrentClient(torrent_dir=d)
                result = client.resolve_stream({"title": "Missing", "duration": 60}, d)
            self.assertIsNone(result)


# ---------------------------------------------------------------------------
# schedule_builder API — path safety
# ---------------------------------------------------------------------------

class TestScheduleBuilderPathSafety(unittest.TestCase):
    """
    Test the path-traversal guard in the schedule builder.
    We replicate the logic here to avoid importing the full module chain
    (which pulls in media_processor → ffmpeg).
    """

    def _safe_resolve_standalone(self, project_root, relative_path):
        """Mirror of schedule_builder._safe_resolve for isolated testing."""
        import os
        clean = relative_path.lstrip("/").lstrip("\\")
        resolved = os.path.realpath(os.path.join(project_root, clean))
        if not resolved.startswith(project_root):
            raise ValueError("Path traversal detected")
        return resolved

    def test_traversal_raises(self):
        root = "/some/project"
        with self.assertRaises(ValueError):
            self._safe_resolve_standalone(root, "../../etc/passwd")

    def test_valid_sub_path(self):
        root = "/some/project"
        result = self._safe_resolve_standalone(root, "catalog/myshow")
        self.assertTrue(result.startswith(root))

    def test_absolute_path_is_neutralised(self):
        """A leading slash is stripped by lstrip so /etc/passwd becomes safe."""
        root = "/some/project"
        # After lstrip('/'), '/etc/passwd' → 'etc/passwd' → '/some/project/etc/passwd'
        # That stays inside root — no error raised.
        result = self._safe_resolve_standalone(root, "/etc/passwd")
        self.assertTrue(result.startswith(root))


# ---------------------------------------------------------------------------
# schedule_builder API — schedule normalisation helpers (standalone)
# ---------------------------------------------------------------------------

class TestScheduleNormalisation(unittest.TestCase):
    """Test normalisation helpers independently of the heavy import chain."""

    DAYS = ['monday','tuesday','wednesday','thursday','friday','saturday','sunday']
    HOURS = list(range(24))

    def _normalize_standard_schedule(self, raw_conf):
        """Replicated from schedule_builder._normalize_standard_schedule."""
        schedule = {}
        day_templates = raw_conf.get("day_templates", {})
        for day in self.DAYS:
            day_val = raw_conf.get(day, {})
            if isinstance(day_val, str):
                day_val = day_templates.get(day_val, {})
            schedule[day] = {str(h): day_val.get(str(h), {}) for h in self.HOURS}
        return schedule

    def _normalize_torrent_schedule(self, raw_conf):
        return raw_conf.get("torrent_streams", [])

    def test_normalize_standard_fills_all_hours(self):
        raw = {
            "monday": {"8": {"tags": "morning"}, "20": {"tags": "prime"}},
            "tuesday": {"9": {"tags": "cartoon"}},
        }
        sched = self._normalize_standard_schedule(raw)
        for day in self.DAYS:
            self.assertIn(day, sched)
            for h in self.HOURS:
                self.assertIn(str(h), sched[day])

    def test_normalize_standard_preserves_slots(self):
        raw = {"monday": {"8": {"tags": "morning"}, "3": {"event": "signoff"}}}
        sched = self._normalize_standard_schedule(raw)
        self.assertEqual(sched["monday"]["8"], {"tags": "morning"})
        self.assertEqual(sched["monday"]["3"], {"event": "signoff"})

    def test_normalize_standard_empty_slot_for_unset_hours(self):
        raw = {"friday": {"20": {"tags": "prime"}}}
        sched = self._normalize_standard_schedule(raw)
        self.assertEqual(sched["friday"]["5"], {})   # unset hour returns empty dict

    def test_normalize_standard_day_template_expansion(self):
        raw = {
            "day_templates": {"weekday": {"8": {"tags": "morning"}}},
            "monday": "weekday",
            "tuesday": "weekday",
        }
        sched = self._normalize_standard_schedule(raw)
        self.assertEqual(sched["monday"]["8"], {"tags": "morning"})
        self.assertEqual(sched["tuesday"]["8"], {"tags": "morning"})

    def test_normalize_torrent_returns_list(self):
        conf = {"torrent_streams": [
            {"title": "ep1", "duration": 1800, "magnet": "magnet:?xt=urn:btih:"+"a"*40}
        ]}
        result = self._normalize_torrent_schedule(conf)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["title"], "ep1")

    def test_normalize_torrent_empty(self):
        self.assertEqual(self._normalize_torrent_schedule({}), [])


if __name__ == "__main__":
    unittest.main()
