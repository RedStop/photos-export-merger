"""
Tests for scanner.py – discovering (re-encoded, original) pairs.
"""

from __future__ import annotations

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import pytest
from pathlib import Path


@pytest.fixture()
def tmp_video_dir(tmp_path: Path) -> Path:
    """Return a temp directory populated with dummy video files."""
    return tmp_path


def _touch(path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"")
    return path


class TestScanDirectory:
    def test_finds_simple_pair(self, tmp_video_dir: Path):
        from video_meta_sync.scanner import scan_directory

        _touch(tmp_video_dir / "holiday.mov")
        _touch(tmp_video_dir / "holiday.mkv")

        pairs = scan_directory(tmp_video_dir, "mkv")
        assert len(pairs) == 1
        assert pairs[0].reencoded.name == "holiday.mkv"
        assert pairs[0].original.name  == "holiday.mov"

    def test_ignores_file_without_original(self, tmp_video_dir: Path):
        from video_meta_sync.scanner import scan_directory

        _touch(tmp_video_dir / "orphan.mkv")

        pairs = scan_directory(tmp_video_dir, "mkv")
        assert pairs == []

    def test_multiple_originals_skipped(self, tmp_video_dir: Path, caplog):
        from video_meta_sync.scanner import scan_directory
        import logging

        _touch(tmp_video_dir / "clip.mkv")
        _touch(tmp_video_dir / "clip.mov")
        _touch(tmp_video_dir / "clip.mp4")

        with caplog.at_level(logging.ERROR):
            pairs = scan_directory(tmp_video_dir, "mkv")

        assert pairs == []
        assert any("Multiple originals" in r.message for r in caplog.records)

    def test_recursive_scan(self, tmp_video_dir: Path):
        from video_meta_sync.scanner import scan_directory

        sub = tmp_video_dir / "2021" / "april"
        _touch(sub / "party.mov")
        _touch(sub / "party.mkv")

        pairs = scan_directory(tmp_video_dir, "mkv")
        assert len(pairs) == 1
        assert pairs[0].reencoded.parent == sub

    def test_unknown_extension_not_treated_as_original(self, tmp_video_dir: Path):
        from video_meta_sync.scanner import scan_directory

        _touch(tmp_video_dir / "clip.mkv")
        _touch(tmp_video_dir / "clip.txt")   # not a video extension

        pairs = scan_directory(tmp_video_dir, "mkv")
        assert pairs == []

    def test_different_output_ext(self, tmp_video_dir: Path):
        from video_meta_sync.scanner import scan_directory

        _touch(tmp_video_dir / "video.mov")
        _touch(tmp_video_dir / "video.mp4")

        pairs = scan_directory(tmp_video_dir, "mp4")
        assert len(pairs) == 1
        assert pairs[0].reencoded.suffix == ".mp4"
        assert pairs[0].original.suffix  == ".mov"

    def test_output_ext_not_treated_as_original(self, tmp_video_dir: Path):
        """A file with the output extension must not be chosen as the original."""
        from video_meta_sync.scanner import scan_directory

        _touch(tmp_video_dir / "clip.mkv")
        # A second .mkv with a different stem — unrelated, no pair formed.
        _touch(tmp_video_dir / "other.mkv")

        pairs = scan_directory(tmp_video_dir, "mkv")
        assert pairs == []


class TestScanSingleFile:
    def test_finds_pair(self, tmp_video_dir: Path):
        from video_meta_sync.scanner import scan_single_file

        original  = _touch(tmp_video_dir / "birthday.mov")
        reencoded = _touch(tmp_video_dir / "birthday.mkv")

        pair = scan_single_file(reencoded, "mkv")
        assert pair is not None
        assert pair.reencoded == reencoded
        assert pair.original  == original

    def test_returns_none_when_no_original(self, tmp_video_dir: Path):
        from video_meta_sync.scanner import scan_single_file

        reencoded = _touch(tmp_video_dir / "lonely.mkv")
        pair = scan_single_file(reencoded, "mkv")
        assert pair is None

    def test_wrong_extension_logs_warning(self, tmp_video_dir: Path, caplog):
        from video_meta_sync.scanner import scan_single_file
        import logging

        wrong = _touch(tmp_video_dir / "clip.avi")
        _touch(tmp_video_dir / "clip.mov")

        with caplog.at_level(logging.WARNING):
            scan_single_file(wrong, "mkv")

        assert any("expected re-encoded extension" in r.message for r in caplog.records)
