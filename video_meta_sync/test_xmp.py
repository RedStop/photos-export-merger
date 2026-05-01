"""
Tests for xmp.py – sidecar existence check and ExifTool-based sidecar writing.

ExifTool is not available in the CI container, so write_sidecar is tested by
mocking ExifToolHelper and asserting the correct arguments are passed.
"""

from __future__ import annotations

import sys
import os
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from datetime import datetime, timezone, timedelta
from pathlib import Path

import pytest


def _make_meta(offset_hours: int = 2, gps: bool = False):
    from video_meta_sync.metadata import VideoMetadata

    tz = timezone(timedelta(hours=offset_hours))
    utc_dt = datetime(2021, 4, 10, 18, 15, 34, tzinfo=timezone.utc)

    raw_tags: dict = {
        "QuickTime:Keys:Make":    "Apple",
        "QuickTime:Keys:Model":   "iPhone 12 mini",
        "QuickTime:Keys:Software": "14.4",
    }
    gps_tags: dict = {}
    if gps:
        raw_tags["Composite:GPSLatitude"]    = "25 deg 45' 56.52\" S"
        raw_tags["Composite:GPSLongitude"]   = "28 deg 14' 0.60\" E"
        raw_tags["Composite:GPSAltitude"]    = "1395.374 m"
        raw_tags["Composite:GPSAltitudeRef"] = "Above Sea Level"
        gps_tags = {k: v for k, v in raw_tags.items()
                    if k.startswith("Composite:GPS")}

    return VideoMetadata(
        raw_tags=raw_tags,
        original_time_taken=utc_dt,
        recording_tz=tz,
        gps_tags=gps_tags,
    )


def _mock_et_ctx():
    """Return a context-manager mock that yields a mock ExifToolHelper."""
    mock_et  = MagicMock()
    mock_ctx = MagicMock()
    mock_ctx.__enter__ = MagicMock(return_value=mock_et)
    mock_ctx.__exit__  = MagicMock(return_value=False)
    return mock_ctx, mock_et


class TestSidecarExists:
    def test_false_when_no_sidecar(self, tmp_path: Path):
        from video_meta_sync.xmp import sidecar_exists

        video = tmp_path / "clip.mkv"
        video.write_bytes(b"")
        assert sidecar_exists(video) is False

    def test_true_when_sidecar_present(self, tmp_path: Path):
        from video_meta_sync.xmp import sidecar_exists

        video = tmp_path / "clip.mkv"
        video.write_bytes(b"")
        (tmp_path / "clip.mkv.xmp").write_text("<x/>")
        assert sidecar_exists(video) is True


class TestWriteSidecar:
    def test_returns_correct_sidecar_path(self, tmp_path: Path):
        from video_meta_sync.xmp import write_sidecar

        reencoded = tmp_path / "holiday.mkv"
        original  = tmp_path / "holiday.mov"
        reencoded.write_bytes(b"")
        original.write_bytes(b"")
        meta = _make_meta()

        mock_ctx, _ = _mock_et_ctx()
        with patch("video_meta_sync.xmp.exiftool.ExifToolHelper",
                   return_value=mock_ctx):
            sidecar = write_sidecar(reencoded, meta, original)

        assert sidecar.name == "holiday.mkv.xmp"

    def test_dry_run_does_not_call_exiftool(self, tmp_path: Path):
        from video_meta_sync.xmp import write_sidecar

        reencoded = tmp_path / "clip.mkv"
        original  = tmp_path / "clip.mov"
        reencoded.write_bytes(b"")
        original.write_bytes(b"")
        meta = _make_meta()

        with patch("video_meta_sync.xmp.exiftool.ExifToolHelper") as mock_cls:
            write_sidecar(reencoded, meta, original, dry_run=True)
            mock_cls.assert_not_called()

    def test_exiftool_called_with_tagsfromfile(self, tmp_path: Path):
        from video_meta_sync.xmp import write_sidecar

        reencoded = tmp_path / "clip.mkv"
        original  = tmp_path / "clip.mov"
        reencoded.write_bytes(b"")
        original.write_bytes(b"")
        meta = _make_meta()

        mock_ctx, mock_et = _mock_et_ctx()
        with patch("video_meta_sync.xmp.exiftool.ExifToolHelper",
                   return_value=mock_ctx):
            write_sidecar(reencoded, meta, original)

        args = mock_et.execute.call_args[0]
        assert "-tagsfromfile" in args
        assert str(original) in args
        assert str(reencoded) in args

    def test_exiftool_called_with_double_extension_output(self, tmp_path: Path):
        from video_meta_sync.xmp import write_sidecar

        reencoded = tmp_path / "clip.mkv"
        original  = tmp_path / "clip.mov"
        reencoded.write_bytes(b"")
        original.write_bytes(b"")
        meta = _make_meta()

        mock_ctx, mock_et = _mock_et_ctx()
        with patch("video_meta_sync.xmp.exiftool.ExifToolHelper",
                   return_value=mock_ctx):
            write_sidecar(reencoded, meta, original)

        args = mock_et.execute.call_args[0]
        assert "-o" in args
        # The format string that produces e.g. "clip.mkv.xmp"
        assert "%d%f.%e.xmp" in args

    def test_date_override_args_contain_utc_and_local(self, tmp_path: Path):
        from video_meta_sync.xmp import write_sidecar

        reencoded = tmp_path / "clip.mkv"
        original  = tmp_path / "clip.mov"
        reencoded.write_bytes(b"")
        original.write_bytes(b"")
        # UTC+2: 18:15:34Z → 20:15:34+02:00 locally
        meta = _make_meta(offset_hours=2)

        mock_ctx, mock_et = _mock_et_ctx()
        with patch("video_meta_sync.xmp.exiftool.ExifToolHelper",
                   return_value=mock_ctx):
            write_sidecar(reencoded, meta, original)

        args_str = " ".join(mock_et.execute.call_args[0])
        assert "2021:04:10 18:15:34" in args_str   # UTC value
        assert "2021:04:10 20:15:34+02:00" in args_str  # local value

    def test_exiftool_error_is_logged_not_raised(self, tmp_path: Path, caplog):
        import logging
        import exiftool.exceptions
        from video_meta_sync.xmp import write_sidecar

        reencoded = tmp_path / "clip.mkv"
        original  = tmp_path / "clip.mov"
        reencoded.write_bytes(b"")
        original.write_bytes(b"")
        meta = _make_meta()

        mock_ctx, mock_et = _mock_et_ctx()
        mock_et.execute.side_effect = exiftool.exceptions.ExifToolExecuteError(
            "failed", b"", b"err", 1
        )

        with patch("video_meta_sync.xmp.exiftool.ExifToolHelper",
                   return_value=mock_ctx):
            with caplog.at_level(logging.ERROR):
                write_sidecar(reencoded, meta, original)

        assert any("Failed to write sidecar" in r.message for r in caplog.records)
