"""
Tests for metadata.py – datetime parsing and "original time taken" selection.

These tests do NOT require ExifTool to be installed; they exercise the pure
Python time-parsing helpers directly using the raw_tags dicts that would be
returned by exiftool for the two sample files provided in the spec.
"""

from __future__ import annotations

import sys
import os

# Allow running tests from the repo root: python -m pytest tests/
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from datetime import datetime, timezone, timedelta

import pytest

from video_meta_sync.metadata import (
    _parse_exif_datetime,
    _to_utc,
    _select_earliest_time,
)


# ---------------------------------------------------------------------------
# _parse_exif_datetime
# ---------------------------------------------------------------------------

class TestParseExifDatetime:
    def test_naive(self):
        dt = _parse_exif_datetime("2021:04:10 18:15:34")
        assert dt is not None
        assert dt.year == 2021
        assert dt.month == 4
        assert dt.day == 10
        assert dt.hour == 18
        assert dt.tzinfo is None

    def test_aware_positive_offset(self):
        dt = _parse_exif_datetime("2021:04:10 20:15:34+02:00")
        assert dt is not None
        assert dt.tzinfo is not None
        # UTC equivalent should be 18:15:34
        utc = dt.astimezone(timezone.utc)
        assert utc.hour == 18
        assert utc.minute == 15

    def test_aware_z_suffix(self):
        dt = _parse_exif_datetime("2021:04:10 18:15:34Z")
        assert dt is not None
        assert dt.tzinfo is not None
        assert dt.astimezone(timezone.utc).hour == 18

    def test_none_on_garbage(self):
        assert _parse_exif_datetime("not-a-date") is None

    def test_none_on_zero_date(self):
        assert _parse_exif_datetime("0000:00:00 00:00:00") is None

    def test_none_on_empty(self):
        assert _parse_exif_datetime("") is None


# ---------------------------------------------------------------------------
# _to_utc
# ---------------------------------------------------------------------------

class TestToUtc:
    def test_naive_treated_as_utc(self):
        dt = datetime(2021, 4, 10, 18, 15, 34)
        utc = _to_utc(dt)
        assert utc.tzinfo == timezone.utc
        assert utc.hour == 18

    def test_aware_converted(self):
        tz_plus2 = timezone(timedelta(hours=2))
        dt = datetime(2021, 4, 10, 20, 15, 34, tzinfo=tz_plus2)
        utc = _to_utc(dt)
        assert utc.hour == 18


# ---------------------------------------------------------------------------
# _select_earliest_time  – using the two sample exiftool outputs from the spec
# ---------------------------------------------------------------------------

# Simulated raw_tags for the Apple .mov file
MOV_RAW_TAGS = {
    "QuickTime:Keys:CreationDate":          "2021:04:10 20:15:34+02:00",
    "File:System:FileCreateDate":           "2026:05:01 00:33:32+02:00",
    "File:System:FileModifyDate":           "2024:12:11 01:31:08+02:00",
    "QuickTime:CreateDate":                 "2021:04:10 18:15:34",        # UTC naive
    "QuickTime:Track1:TrackCreateDate":     "2021:04:10 18:15:34",
    "QuickTime:Track5:MediaCreateDate":     "2021:04:10 18:15:34",
}

# Simulated raw_tags for the Android .mp4 file
MP4_RAW_TAGS = {
    "File:System:FileCreateDate":           "2026:05:01 00:33:51+02:00",
    "File:System:FileModifyDate":           "2024:12:11 01:28:00+02:00",
    "QuickTime:CreateDate":                 "2021:09:03 08:34:34",        # UTC naive
    "QuickTime:Track1:TrackCreateDate":     "2021:09:03 08:34:34",
    "QuickTime:Track2:MediaCreateDate":     "2021:09:03 08:34:34",
}


class TestSelectEarliestTime:
    def test_mov_selects_quicktime_create_date(self):
        """
        For the .mov file the QuickTime:CreateDate / Keys:CreationDate both
        resolve to 2021-04-10 18:15:34 UTC which is much earlier than any
        filesystem date.
        """
        result = _select_earliest_time(MOV_RAW_TAGS)
        assert result is not None
        best_utc, recording_tz = result
        assert best_utc.year  == 2021
        assert best_utc.month == 4
        assert best_utc.day   == 10
        assert best_utc.hour  == 18
        assert best_utc.minute == 15

    def test_mov_recording_tz_is_plus2(self):
        """
        The earliest field with a timezone offset is Keys:CreationDate (+02:00),
        so the recording timezone should be UTC+2.
        """
        result = _select_earliest_time(MOV_RAW_TAGS)
        assert result is not None
        _, recording_tz = result
        # UTC+2 expressed as a fixed offset
        expected_offset = timedelta(hours=2)
        # The recording tz offset should be +02:00
        sample_dt = datetime(2021, 4, 10, 20, 15, 34, tzinfo=recording_tz)
        assert sample_dt.utcoffset() == expected_offset

    def test_mp4_selects_quicktime_create_date(self):
        """
        For the .mp4 file (Android), QuickTime:CreateDate 2021:09:03 08:34:34
        (UTC) is earlier than both filesystem dates from 2024/2026.
        """
        result = _select_earliest_time(MP4_RAW_TAGS)
        assert result is not None
        best_utc, _ = result
        assert best_utc.year   == 2021
        assert best_utc.month  == 9
        assert best_utc.day    == 3
        assert best_utc.hour   == 8
        assert best_utc.minute == 34

    def test_mp4_no_tz_offset_falls_back_to_local(self):
        """
        The Android .mp4 has no timezone-aware field among the video metadata,
        so recording_tz should fall back to the local machine timezone (not
        UTC, not a fixed offset we can assert exactly – just verify it is set).
        """
        result = _select_earliest_time(MP4_RAW_TAGS)
        assert result is not None
        _, recording_tz = result
        assert recording_tz is not None

    def test_empty_tags_returns_none(self):
        assert _select_earliest_time({}) is None

    def test_only_garbage_dates_returns_none(self):
        tags = {"QuickTime:CreateDate": "0000:00:00 00:00:00"}
        assert _select_earliest_time(tags) is None

    def test_prefers_earlier_of_two_utc_dates(self):
        tags = {
            "QuickTime:CreateDate":             "2020:01:01 10:00:00",
            "QuickTime:Track1:TrackCreateDate": "2019:06:15 08:00:00",
        }
        result = _select_earliest_time(tags)
        assert result is not None
        best_utc, _ = result
        assert best_utc.year  == 2019
        assert best_utc.month == 6


# ---------------------------------------------------------------------------
# VideoMetadata convenience properties
# ---------------------------------------------------------------------------

class TestVideoMetadataProperties:
    """Test the string-formatting helpers on VideoMetadata."""

    def _make_meta(self, offset_hours: int):
        from video_meta_sync.metadata import VideoMetadata
        tz = timezone(timedelta(hours=offset_hours))
        utc_dt = datetime(2021, 4, 10, 18, 15, 34, tzinfo=timezone.utc)
        return VideoMetadata(
            raw_tags={},
            original_time_taken=utc_dt,
            recording_tz=tz,
        )

    def test_utc_str(self):
        meta = self._make_meta(2)
        assert meta.original_time_utc_str == "2021:04:10 18:15:34"

    def test_local_str_plus2(self):
        meta = self._make_meta(2)
        # 18:15:34 UTC → 20:15:34+02:00
        assert meta.original_time_local_str == "2021:04:10 20:15:34+02:00"

    def test_local_str_minus5(self):
        meta = self._make_meta(-5)
        # 18:15:34 UTC → 13:15:34-05:00
        assert meta.original_time_local_str == "2021:04:10 13:15:34-05:00"

    def test_utc_str_no_offset_suffix(self):
        meta = self._make_meta(0)
        assert "+" not in meta.original_time_utc_str
        assert "-" not in meta.original_time_utc_str
