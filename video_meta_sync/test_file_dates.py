"""
Tests for file_dates.py – filesystem timestamp updates via ExifTool.

ExifTool itself is not available in the CI container, so these tests mock
the ExifToolHelper to verify that the correct arguments are passed and that
dry-run mode suppresses the call entirely.
"""

from __future__ import annotations

import sys
import os
from unittest.mock import MagicMock, patch, call

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from datetime import datetime, timezone, timedelta
from pathlib import Path

import pytest


def _make_meta(offset_hours: int = 2):
    from video_meta_sync.metadata import VideoMetadata

    tz = timezone(timedelta(hours=offset_hours))
    utc_dt = datetime(2021, 4, 10, 18, 15, 34, tzinfo=timezone.utc)
    return VideoMetadata(
        raw_tags={},
        original_time_taken=utc_dt,
        recording_tz=tz,
    )


class TestUpdateFileDates:
    def test_calls_exiftool_with_correct_date_string(self, tmp_path: Path):
        from video_meta_sync.file_dates import update_file_dates

        video = tmp_path / "clip.mkv"
        video.write_bytes(b"dummy")
        meta = _make_meta(offset_hours=2)

        mock_et = MagicMock()
        mock_ctx = MagicMock()
        mock_ctx.__enter__ = MagicMock(return_value=mock_et)
        mock_ctx.__exit__ = MagicMock(return_value=False)

        with patch("video_meta_sync.file_dates.exiftool.ExifToolHelper",
                   return_value=mock_ctx):
            update_file_dates(video, meta, dry_run=False)

        # 18:15:34 UTC → 20:15:34+02:00 in recording tz
        expected_date = "2021:04:10 20:15:34+02:00"
        mock_et.execute.assert_called_once_with(
            f"-FileModifyDate={expected_date}",
            f"-FileCreateDate={expected_date}",
            str(video),
        )

    def test_dry_run_does_not_call_exiftool(self, tmp_path: Path):
        from video_meta_sync.file_dates import update_file_dates

        video = tmp_path / "clip.mkv"
        video.write_bytes(b"dummy")
        meta = _make_meta(offset_hours=2)

        with patch("video_meta_sync.file_dates.exiftool.ExifToolHelper") as mock_cls:
            update_file_dates(video, meta, dry_run=True)
            mock_cls.assert_not_called()

    def test_negative_offset(self, tmp_path: Path):
        from video_meta_sync.file_dates import update_file_dates

        video = tmp_path / "clip.mkv"
        video.write_bytes(b"dummy")
        meta = _make_meta(offset_hours=-5)

        mock_et = MagicMock()
        mock_ctx = MagicMock()
        mock_ctx.__enter__ = MagicMock(return_value=mock_et)
        mock_ctx.__exit__ = MagicMock(return_value=False)

        with patch("video_meta_sync.file_dates.exiftool.ExifToolHelper",
                   return_value=mock_ctx):
            update_file_dates(video, meta, dry_run=False)

        # 18:15:34 UTC → 13:15:34-05:00
        expected_date = "2021:04:10 13:15:34-05:00"
        mock_et.execute.assert_called_once_with(
            f"-FileModifyDate={expected_date}",
            f"-FileCreateDate={expected_date}",
            str(video),
        )

    def test_exiftool_error_is_logged_not_raised(self, tmp_path: Path, caplog):
        import logging
        from video_meta_sync.file_dates import update_file_dates
        import exiftool.exceptions

        video = tmp_path / "clip.mkv"
        video.write_bytes(b"dummy")
        meta = _make_meta(offset_hours=0)

        mock_et = MagicMock()
        mock_et.execute.side_effect = exiftool.exceptions.ExifToolExecuteError(
            "exiftool failed", b"", b"error", 1
        )
        mock_ctx = MagicMock()
        mock_ctx.__enter__ = MagicMock(return_value=mock_et)
        mock_ctx.__exit__ = MagicMock(return_value=False)

        with patch("video_meta_sync.file_dates.exiftool.ExifToolHelper",
                   return_value=mock_ctx):
            with caplog.at_level(logging.ERROR):
                update_file_dates(video, meta, dry_run=False)

        assert any("Failed to set file dates" in r.message for r in caplog.records)
