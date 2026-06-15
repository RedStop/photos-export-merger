"""Tests for the reencode_av1 package."""

from __future__ import annotations

import argparse
import json
import math
import tempfile
from pathlib import Path
from unittest import mock

import pytest

from reencode_av1.filters import (
    BitrateWindows,
    _get_scale_filter,
    build_extra_args,
    compute_segment_offsets,
    compute_windows,
)
from reencode_av1.probe import (
    VideoInfo,
    _parse_fraction,
    get_total_bitrate,
    get_video_info,
    measure_overhead,
)
from reencode_av1.encode import (
    _base_encode_args,
    _extract_vf_filter,
    _parse_time_to_seconds,
)
from reencode_av1.search import (
    CrfPoint,
    CrfResult,
    SearchContext,
    binary_search_next,
    find_optimal_crf,
    interpolate_crf,
    interpolation_next,
    smart_search_next,
)
from reencode_av1.__main__ import (
    FileResult,
    build_parser,
    compute_audio_bitrate,
    get_output_path,
    validate_args,
    process_file,
)
from reencode_av1.progress import (
    PROGRESS_FILENAME,
    load_progress,
    progress_path_for,
    record_progress,
)


# ═══════════════════════════════════════════════════════════════════════════════
# probe.py tests
# ═══════════════════════════════════════════════════════════════════════════════


class TestParseFraction:
    def test_standard(self):
        assert _parse_fraction("30000/1001") == pytest.approx(29.97, abs=0.01)

    def test_integer(self):
        assert _parse_fraction("30/1") == 30.0

    def test_zero_denominator(self):
        assert _parse_fraction("30/0") == 0.0

    def test_zero_over_zero(self):
        assert _parse_fraction("0/0") == 0.0

    def test_not_a_fraction(self):
        assert _parse_fraction("30") == 0.0

    def test_empty(self):
        assert _parse_fraction("") == 0.0


class TestGetVideoInfo:
    """Test get_video_info with mocked ffprobe output."""

    def _make_probe_output(self, **overrides):
        """Build a realistic ffprobe JSON structure."""
        base = {
            "streams": [
                {
                    "codec_type": "video",
                    "codec_name": "h264",
                    "width": 1920,
                    "height": 1080,
                    "avg_frame_rate": "30/1",
                    "r_frame_rate": "30/1",
                    "bit_rate": "5000000",
                },
                {
                    "codec_type": "audio",
                    "codec_name": "aac",
                    "channels": 2,
                    "bit_rate": "128000",
                },
            ],
            "format": {
                "duration": "120.0",
                "bit_rate": "5128000",
            },
        }
        base.update(overrides)
        return base

    @mock.patch("reencode_av1.probe.run_ffprobe")
    def test_basic(self, mock_probe):
        mock_probe.return_value = self._make_probe_output()
        info = get_video_info(Path("test.mp4"))
        assert info is not None
        assert info.codec == "h264"
        assert info.width == 1920
        assert info.height == 1080
        assert info.fps == 30.0
        assert info.is_vfr is False
        assert info.bitrate_kbps == 5000
        assert info.total_bitrate_kbps == 5128  # format bit_rate 5128000
        assert info.duration_sec == 120.0
        assert info.audio_channels == 2
        assert info.audio_codec == "aac"

    @mock.patch("reencode_av1.probe.run_ffprobe")
    def test_vfr_detection(self, mock_probe):
        data = self._make_probe_output()
        data["streams"][0]["avg_frame_rate"] = "24000/1001"
        data["streams"][0]["r_frame_rate"] = "30000/1001"
        mock_probe.return_value = data
        info = get_video_info(Path("test.mp4"))
        assert info is not None
        assert info.is_vfr is True

    @mock.patch("reencode_av1.probe.run_ffprobe")
    def test_no_video_stream(self, mock_probe):
        data = self._make_probe_output()
        data["streams"] = [data["streams"][1]]  # audio only
        mock_probe.return_value = data
        assert get_video_info(Path("test.mp4")) is None

    @mock.patch("reencode_av1.probe.run_ffprobe")
    def test_no_stream_bitrate_falls_back_to_format(self, mock_probe):
        data = self._make_probe_output()
        del data["streams"][0]["bit_rate"]
        mock_probe.return_value = data
        info = get_video_info(Path("test.mp4"))
        assert info is not None
        # format bitrate 5128 - audio 128 = 5000
        assert info.bitrate_kbps == 5000

    @mock.patch("reencode_av1.probe.run_ffprobe")
    def test_no_audio_stream(self, mock_probe):
        data = self._make_probe_output()
        data["streams"] = [data["streams"][0]]  # video only
        mock_probe.return_value = data
        info = get_video_info(Path("test.mp4"))
        assert info is not None
        assert info.audio_channels == 2  # default
        assert info.audio_codec is None

    @mock.patch("reencode_av1.probe.run_ffprobe")
    def test_ffprobe_failure(self, mock_probe):
        mock_probe.side_effect = json.JSONDecodeError("err", "", 0)
        assert get_video_info(Path("test.mp4")) is None

    @mock.patch("reencode_av1.probe.run_ffprobe")
    def test_fallback_bitrate_no_audio_bitrate(self, mock_probe):
        """When audio stream exists but has no bitrate, estimate 128 kbps."""
        data = self._make_probe_output()
        del data["streams"][0]["bit_rate"]
        del data["streams"][1]["bit_rate"]
        mock_probe.return_value = data
        info = get_video_info(Path("test.mp4"))
        assert info is not None
        # format 5128 - audio estimate 128 = 5000
        assert info.bitrate_kbps == 5000


class TestGetTotalBitrate:
    @mock.patch("reencode_av1.probe.run_ffprobe")
    def test_format_bitrate(self, mock_probe):
        mock_probe.return_value = {
            "streams": [{"codec_type": "video"}],
            "format": {"duration": "60", "bit_rate": "3128000"},
        }
        # Whole-file bitrate straight from the container.
        assert get_total_bitrate(Path("out.mkv")) == 3128

    @mock.patch("reencode_av1.probe.run_ffprobe")
    def test_fallback_filesize(self, mock_probe):
        mock_probe.return_value = {
            "streams": [{"codec_type": "video"}],
            "format": {"duration": "10"},
        }
        with tempfile.NamedTemporaryFile(suffix=".mkv", delete=False) as f:
            f.write(b"\x00" * 1_000_000)
            tmp = Path(f.name)
        try:
            # file_size=1000000 bytes, duration=10s → 1000000*8/10/1000 = 800
            assert get_total_bitrate(tmp) == 800
        finally:
            tmp.unlink(missing_ok=True)

    @mock.patch("reencode_av1.probe.run_ffprobe")
    def test_zero_duration(self, mock_probe):
        mock_probe.return_value = {
            "streams": [{"codec_type": "video"}],
            "format": {"duration": "0"},
        }
        assert get_total_bitrate(Path("out.mkv")) == -1

    @mock.patch("reencode_av1.probe.run_ffprobe")
    def test_duration_hint_used_when_format_lacks_it(self, mock_probe):
        mock_probe.return_value = {"streams": [{"codec_type": "video"}], "format": {}}
        with tempfile.NamedTemporaryFile(suffix=".mkv", delete=False) as f:
            f.write(b"\x00" * 1_000_000)
            tmp = Path(f.name)
        try:
            assert get_total_bitrate(tmp, duration_hint=10) == 800
        finally:
            tmp.unlink(missing_ok=True)

    @mock.patch("reencode_av1.probe.run_ffprobe")
    def test_probe_failure(self, mock_probe):
        mock_probe.side_effect = json.JSONDecodeError("err", "", 0)
        assert get_total_bitrate(Path("out.mkv")) == -1


class TestMeasureOverhead:
    @mock.patch("reencode_av1.probe._sum_video_packet_bytes")
    @mock.patch("reencode_av1.probe.run_ffprobe")
    def test_overhead_is_total_minus_video(self, mock_probe, mock_video_bytes):
        # total 2628 kbps; video packets sum to 2500 kbps over 10s → overhead 128.
        mock_probe.return_value = {"format": {"duration": "10", "bit_rate": "2628000"}}
        mock_video_bytes.return_value = 2500 * 1000 * 10 // 8  # bytes for 2500 kbps
        assert measure_overhead(Path("out.mkv")) == 128

    @mock.patch("reencode_av1.probe._sum_video_packet_bytes")
    @mock.patch("reencode_av1.probe.run_ffprobe")
    def test_no_audio_small_overhead(self, mock_probe, mock_video_bytes):
        # No audio: overhead is just container muxing (here 10 kbps).
        mock_probe.return_value = {"format": {"duration": "10", "bit_rate": "2510000"}}
        mock_video_bytes.return_value = 2500 * 1000 * 10 // 8
        assert measure_overhead(Path("out.mkv")) == 10

    @mock.patch("reencode_av1.probe._sum_video_packet_bytes")
    @mock.patch("reencode_av1.probe.run_ffprobe")
    def test_fallback_total_from_filesize(self, mock_probe, mock_video_bytes):
        mock_probe.return_value = {"format": {"duration": "10"}}  # no bit_rate
        with tempfile.NamedTemporaryFile(suffix=".mkv", delete=False) as f:
            f.write(b"\x00" * 1_000_000)  # 800 kbps total over 10s
            tmp = Path(f.name)
        try:
            mock_video_bytes.return_value = 700 * 1000 * 10 // 8  # 700 kbps video
            assert measure_overhead(tmp) == 100
        finally:
            tmp.unlink(missing_ok=True)

    @mock.patch("reencode_av1.probe._sum_video_packet_bytes")
    @mock.patch("reencode_av1.probe.run_ffprobe")
    def test_clamps_to_zero(self, mock_probe, mock_video_bytes):
        # Video packets summing above the total (noise) must not go negative.
        mock_probe.return_value = {"format": {"duration": "10", "bit_rate": "2000000"}}
        mock_video_bytes.return_value = 2100 * 1000 * 10 // 8
        assert measure_overhead(Path("out.mkv")) == 0

    @mock.patch("reencode_av1.probe._sum_video_packet_bytes")
    @mock.patch("reencode_av1.probe.run_ffprobe")
    def test_returns_none_when_packets_unavailable(self, mock_probe, mock_video_bytes):
        mock_probe.return_value = {"format": {"duration": "10", "bit_rate": "2000000"}}
        mock_video_bytes.return_value = None
        assert measure_overhead(Path("out.mkv")) is None

    @mock.patch("reencode_av1.probe.run_ffprobe")
    def test_returns_none_on_probe_failure(self, mock_probe):
        mock_probe.side_effect = json.JSONDecodeError("err", "", 0)
        assert measure_overhead(Path("out.mkv")) is None


# ═══════════════════════════════════════════════════════════════════════════════
# filters.py tests
# ═══════════════════════════════════════════════════════════════════════════════


class TestGetScaleFilter:
    def test_1080p_no_scale(self):
        assert _get_scale_filter(1920, 1080) is None

    def test_720p_no_scale(self):
        assert _get_scale_filter(1280, 720) is None

    def test_4k_landscape(self):
        assert _get_scale_filter(3840, 2160) == "scale=-2:1080"

    def test_wide_landscape(self):
        """Width > 1920 but height <= 1080."""
        assert _get_scale_filter(2560, 1080) == "scale=1920:-2"

    def test_4k_portrait(self):
        assert _get_scale_filter(2160, 3840) == "scale=1080:-2"

    def test_tall_portrait(self):
        """Height > 1920 but width <= 1080."""
        assert _get_scale_filter(1080, 2560) == "scale=-2:1920"

    def test_square_no_scale(self):
        assert _get_scale_filter(1080, 1080) is None

    def test_square_large(self):
        assert _get_scale_filter(1440, 1440) == "scale=1080:-2"

    def test_small_video(self):
        assert _get_scale_filter(640, 480) is None


class TestBuildExtraArgs:
    def test_standard_1080p(self):
        info = VideoInfo("h264", 1920, 1080, 30.0, False, 5000, 120.0, 3600, 2, "aac")
        args = build_extra_args(info)
        assert "-vf" not in args
        assert "-g" in args
        assert "-keyint_min" in args
        g_idx = args.index("-g")
        assert args[g_idx + 1] == "240"  # 30 * 8
        k_idx = args.index("-keyint_min")
        assert args[k_idx + 1] == "120"  # 30 * 4

    def test_4k_adds_scale(self):
        info = VideoInfo("h264", 3840, 2160, 30.0, False, 15000, 120.0, 3600, 2, "aac")
        args = build_extra_args(info)
        assert "-vf" in args
        vf_idx = args.index("-vf")
        assert args[vf_idx + 1] == "scale=-2:1080"

    def test_zero_fps_defaults_to_30(self):
        info = VideoInfo("h264", 1920, 1080, 0.0, False, 5000, 120.0, 3600, 2, "aac")
        args = build_extra_args(info)
        g_idx = args.index("-g")
        assert args[g_idx + 1] == "240"  # 30 * 8

    def test_60fps_gop(self):
        info = VideoInfo("h264", 1920, 1080, 60.0, False, 5000, 120.0, 3600, 2, "aac")
        args = build_extra_args(info)
        g_idx = args.index("-g")
        assert args[g_idx + 1] == "480"  # 60 * 8
        k_idx = args.index("-keyint_min")
        assert args[k_idx + 1] == "240"  # 60 * 4


class TestComputeSegmentOffsets:
    def test_basic(self):
        offsets = compute_segment_offsets(120.0, 5, 3.0)
        assert len(offsets) == 5
        # All segments must fit within the video
        for o in offsets:
            assert o >= 0.0
            assert o + 3.0 <= 120.0

    def test_evenly_spaced(self):
        offsets = compute_segment_offsets(120.0, 5, 3.0)
        # Centres should be at 20, 40, 60, 80, 100
        for i, o in enumerate(offsets):
            expected_centre = 120.0 * (i + 1) / 6
            assert abs((o + 1.5) - expected_centre) < 0.01

    def test_single_segment(self):
        offsets = compute_segment_offsets(60.0, 1, 3.0)
        assert len(offsets) == 1
        # Centre at 30, start at 28.5
        assert offsets[0] == pytest.approx(28.5, abs=0.01)

    def test_short_video_clamped(self):
        """When video is barely longer than segments, offsets should clamp."""
        offsets = compute_segment_offsets(10.0, 3, 3.0)
        assert len(offsets) == 3
        for o in offsets:
            assert o >= 0.0
            assert o + 3.0 <= 10.0

    def test_segment_at_boundary(self):
        """When a segment would overshoot the end, it gets clamped."""
        offsets = compute_segment_offsets(15.0, 5, 3.0)
        assert len(offsets) == 5
        for o in offsets:
            assert o >= 0.0
            assert o + 3.0 <= 15.0


class TestComputeWindows:
    def test_defaults(self):
        # target=2500, allowed=1500, target_window=100, buffer=50
        w = compute_windows(2500, 1500, 100, 50)
        assert w.target == 2500
        assert w.sample_lo == 1050   # 2500 - 1500 + 50
        assert w.sample_hi == 2450   # 2500 - 50
        assert w.sample_confident_lo == 2450  # 2500 - 100 + 50
        assert w.sample_confident_hi == 2450  # 2500 - 50
        assert w.final_lo == 1000    # 2500 - 1500
        assert w.final_hi == 2500
        assert w.final_accept_lo == 2400  # 2500 - 100
        assert w.final_accept_hi == 2500

    def test_low_target_clamps_to_zero(self):
        w = compute_windows(100, 500, 100, 50)
        assert w.sample_lo == 0
        assert w.final_lo == 0

    def test_sample_window_inside_final(self):
        """Sample window should always be narrower than or equal to final."""
        w = compute_windows(2500, 500, 100, 50)
        assert w.sample_lo >= w.final_lo
        assert w.sample_hi <= w.final_hi

    def test_confident_inside_sample(self):
        """Confident zone should be inside the sample window."""
        w = compute_windows(2500, 500, 100, 50)
        assert w.sample_confident_lo >= w.sample_lo
        assert w.sample_confident_hi <= w.sample_hi

    def test_final_hi_equals_target(self):
        """We should never accept a final bitrate above target."""
        for target in [500, 1000, 2500, 5000, 10000]:
            w = compute_windows(target, 500, 100, 50)
            assert w.final_hi == target
            assert w.final_accept_hi == target


# ═══════════════════════════════════════════════════════════════════════════════
# encode.py tests
# ═══════════════════════════════════════════════════════════════════════════════


class TestBaseEncodeArgs:
    def test_includes_codec_and_crf(self):
        args = _base_encode_args(30, [], "128k", 3)
        assert "-c:v" in args
        assert "libsvtav1" in args
        assert "-crf" in args
        crf_idx = args.index("-crf")
        assert args[crf_idx + 1] == "30"

    def test_preset(self):
        args = _base_encode_args(30, [], "128k", 6)
        preset_idx = args.index("-preset")
        assert args[preset_idx + 1] == "6"

    def test_extra_args_prepended(self):
        args = _base_encode_args(30, ["-g", "240"], "128k", 3)
        assert args[0] == "-g"
        assert args[1] == "240"

    def test_audio_settings(self):
        args = _base_encode_args(30, [], "96k", 3)
        assert "-c:a" in args
        assert "libopus" in args
        ba_idx = args.index("-b:a")
        assert args[ba_idx + 1] == "96k"


class TestExtractVfFilter:
    def test_no_vf(self):
        vf, remaining = _extract_vf_filter(["-g", "240", "-keyint_min", "120"])
        assert vf is None
        assert remaining == ["-g", "240", "-keyint_min", "120"]

    def test_with_vf(self):
        vf, remaining = _extract_vf_filter(
            ["-vf", "scale=-2:1080", "-g", "240"]
        )
        assert vf == "scale=-2:1080"
        assert remaining == ["-g", "240"]

    def test_vf_at_end(self):
        vf, remaining = _extract_vf_filter(["-g", "240", "-vf", "scale=1920:-2"])
        assert vf == "scale=1920:-2"
        assert remaining == ["-g", "240"]

    def test_vf_without_value(self):
        """If -vf is the last arg with no value, it's kept as-is."""
        vf, remaining = _extract_vf_filter(["-g", "240", "-vf"])
        assert vf is None
        assert remaining == ["-g", "240", "-vf"]

    def test_empty(self):
        vf, remaining = _extract_vf_filter([])
        assert vf is None
        assert remaining == []


class TestParseTimeToSeconds:
    def test_basic(self):
        assert _parse_time_to_seconds("00:01:23.45") == pytest.approx(83.45, abs=0.01)

    def test_hours(self):
        assert _parse_time_to_seconds("01:00:00.00") == pytest.approx(3600.0)

    def test_zero(self):
        assert _parse_time_to_seconds("00:00:00.00") == 0.0

    def test_no_fractional(self):
        assert _parse_time_to_seconds("00:02:30") == pytest.approx(150.0)

    def test_short_string(self):
        assert _parse_time_to_seconds("12") == 0.0

    def test_empty(self):
        assert _parse_time_to_seconds("") == 0.0


# ═══════════════════════════════════════════════════════════════════════════════
# search.py tests
# ═══════════════════════════════════════════════════════════════════════════════


class TestInterpolateCrf:
    def test_basic_interpolation(self):
        # CRF 20 -> 5000 kbps, CRF 40 -> 1000 kbps, target 2500
        result = interpolate_crf(CrfPoint(20, 5000), CrfPoint(40, 1000), 2500, 1, 63)
        assert 1 <= result <= 63
        # Should be between 20 and 40
        assert 20 <= result <= 40

    def test_clamped_to_min(self):
        # Target much higher than both points -> clamp to min
        result = interpolate_crf(CrfPoint(20, 100), CrfPoint(40, 50), 50000, 5, 63)
        assert result == 5

    def test_clamped_to_max(self):
        # Target much lower than both points -> clamp to max
        result = interpolate_crf(CrfPoint(20, 50000), CrfPoint(40, 30000), 10, 1, 63)
        assert result == 63

    def test_equal_bitrates_returns_midpoint(self):
        result = interpolate_crf(CrfPoint(20, 2000), CrfPoint(40, 2000), 2500, 1, 63)
        assert result == 30

    def test_zero_bitrate_returns_midpoint(self):
        result = interpolate_crf(CrfPoint(20, 0), CrfPoint(40, 2000), 2500, 1, 63)
        assert result == 30

    def test_log_linear_relationship(self):
        """Verify interpolation uses log-space, not linear."""
        # Two points: CRF 10 -> 10000, CRF 50 -> 100
        # Target: 1000 (geometric mean of 10000 and 100)
        # In log-space, 1000 is exactly at the midpoint of log(10000) and log(100)
        result = interpolate_crf(CrfPoint(10, 10000), CrfPoint(50, 100), 1000, 1, 63)
        assert result == 30  # exact midpoint in log-space

    def test_overhead_subtracted_before_log(self):
        """With overhead, interpolation runs on the video component (total-overhead)."""
        # Totals 10000 and 100 with overhead 50 → video 9950 and 50.
        # Video target 950 (= 1000 - 50) is the geometric mean of 9950... not quite,
        # so just assert it differs from the overhead-free midpoint and stays bracketed.
        with_oh = interpolate_crf(CrfPoint(10, 10000), CrfPoint(50, 100), 1000, 1, 63, overhead=50)
        assert 10 <= with_oh <= 50

    def test_overhead_zero_matches_default(self):
        a = interpolate_crf(CrfPoint(20, 5000), CrfPoint(40, 1000), 2500, 1, 63)
        b = interpolate_crf(CrfPoint(20, 5000), CrfPoint(40, 1000), 2500, 1, 63, overhead=0)
        assert a == b

    def test_overhead_exceeds_video_returns_midpoint(self):
        # Overhead larger than a point's bitrate → non-positive video → midpoint fallback.
        result = interpolate_crf(CrfPoint(20, 400), CrfPoint(40, 200), 2500, 1, 63, overhead=500)
        assert result == 30

    def test_overhead_changes_estimate(self):
        """Accounting for a constant offset moves the estimate off the naive fit."""
        no_oh = interpolate_crf(CrfPoint(10, 10000), CrfPoint(50, 1000), 2000, 1, 63)
        with_oh = interpolate_crf(CrfPoint(10, 10000), CrfPoint(50, 1000), 2000, 1, 63, overhead=900)
        assert with_oh != no_oh
        assert 10 <= with_oh <= 50


class TestBinarySearchNext:
    """Stateless binary-search next-CRF picker."""

    def _ctx(self, seed_crf=-1):
        return SearchContext(1, 63, 1000, 2000, seed_crf=seed_crf)

    def test_empty_history_returns_midpoint(self):
        assert binary_search_next([], self._ctx()) == 32

    def test_empty_history_with_seed_uses_seed(self):
        assert binary_search_next([], self._ctx(seed_crf=40)) == 40

    def test_overshoot_raises_lo(self):
        # CRF 20 overshoots → next probe must be > 20
        nxt = binary_search_next([CrfPoint(20, 5000)], self._ctx())
        assert nxt is not None and nxt > 20

    def test_undershoot_lowers_hi(self):
        # CRF 50 undershoots → next probe must be < 50
        nxt = binary_search_next([CrfPoint(50, 500)], self._ctx())
        assert nxt is not None and nxt < 50

    def test_in_range_narrows_toward_lower_crf(self):
        # CRF 30 in range → next probe must be < 30 (looking for higher quality)
        nxt = binary_search_next([CrfPoint(30, 1500)], self._ctx())
        assert nxt is not None and nxt < 30

    def test_consecutive_bracket_returns_none(self):
        # CRF 30 overshoots, CRF 31 in range → no integer between them
        nxt = binary_search_next([CrfPoint(30, 2500), CrfPoint(31, 1500)], self._ctx())
        assert nxt is None


class TestInterpolationNext:
    """Stateless log-linear interpolation next-CRF picker."""

    def _ctx(self, seed_crf=-1, overhead=0):
        return SearchContext(1, 63, 1000, 2000, seed_crf=seed_crf, overhead=overhead)

    def test_empty_history_default_probe(self):
        nxt = interpolation_next([], self._ctx())
        assert nxt == 30

    def test_empty_history_uses_seed(self):
        nxt = interpolation_next([], self._ctx(seed_crf=40))
        assert nxt is not None and 1 <= nxt <= 63

    def test_single_overshoot_probes_higher_crf(self):
        nxt = interpolation_next([CrfPoint(20, 5000)], self._ctx())
        assert nxt is not None and nxt > 20

    def test_single_undershoot_probes_lower_crf(self):
        nxt = interpolation_next([CrfPoint(50, 500)], self._ctx())
        assert nxt is not None and nxt < 50

    def test_bracketed_interpolates(self):
        # CRF 20 overshoots, CRF 50 undershoots — interpolate strictly between
        nxt = interpolation_next(
            [CrfPoint(20, 5000), CrfPoint(50, 500)], self._ctx(),
        )
        assert nxt is not None and 20 < nxt < 50

    def test_consecutive_bracket_returns_none(self):
        nxt = interpolation_next(
            [CrfPoint(30, 2500), CrfPoint(31, 1500)], self._ctx(),
        )
        assert nxt is None

    def test_bracketed_with_overhead_still_between(self):
        nxt = interpolation_next(
            [CrfPoint(20, 5000), CrfPoint(50, 500)], self._ctx(overhead=300),
        )
        assert nxt is not None and 20 < nxt < 50


class TestSmartSearchNext:
    """Stateless bracket-then-interpolate next-CRF picker (the default method).

    Smart search aims at the centre of the confident window, so all cases
    below pass ``confident_lo``/``confident_hi`` (here [2300, 2400], so the
    target is 2350). The ``accept_lo``/``accept_hi`` arguments don't affect
    its decisions but are part of the shared signature.
    """

    CONF_LO = 2300
    CONF_HI = 2400  # target = 2350

    def _next(self, history, crf_min=15, crf_max=57):
        return smart_search_next(
            history,
            SearchContext(crf_min, crf_max, 2050, 2450, self.CONF_LO, self.CONF_HI),
        )

    def test_empty_history_returns_midpoint(self):
        # True midpoint of [crf_min, crf_max], not the half-width.
        assert self._next([]) == (15 + 57) // 2  # 36

    def test_single_overshoot_jumps_to_max(self):
        # bitrate above target → CRF too small → jump to the ceiling to bracket.
        assert self._next([CrfPoint(36, 5000)]) == 57

    def test_single_undershoot_jumps_to_min(self):
        # bitrate below target → CRF too large → jump to the floor to bracket.
        assert self._next([CrfPoint(36, 1000)]) == 15

    def test_bracketed_interpolates_strictly_between(self):
        nxt = self._next([CrfPoint(15, 9000), CrfPoint(57, 500)])
        assert nxt is not None and 15 < nxt < 57

    def test_consecutive_bracket_returns_none(self):
        # CRF 30 overshoots (2500 > 2350), CRF 31 undershoots (2200 <= 2350):
        # no integer CRF between them.
        nxt = self._next([CrfPoint(30, 2500), CrfPoint(31, 2200)])
        assert nxt is None

    def test_all_overshoot_below_ceiling_jumps_to_max(self):
        # Two probes, both overshoot, neither at crf_max → jump to crf_max.
        assert self._next([CrfPoint(20, 6000), CrfPoint(36, 5000)]) == 57

    def test_all_overshoot_at_ceiling_returns_none(self):
        # Even crf_max overshoots → nothing higher to probe.
        nxt = self._next([CrfPoint(36, 5000), CrfPoint(57, 3000)])
        assert nxt is None

    def test_all_undershoot_at_floor_returns_none(self):
        # Even crf_min undershoots → nothing lower to probe.
        nxt = self._next([CrfPoint(36, 2000), CrfPoint(15, 2100)])
        assert nxt is None

    def test_target_is_centre_of_confident_window(self):
        # With confident window [2000, 3000] the target is 2500, so a 2400
        # probe undershoots (jump to floor) even though 2400 would overshoot
        # the [2300, 2400] target used elsewhere.
        nxt = smart_search_next(
            [CrfPoint(36, 2400)],
            SearchContext(15, 57, 2050, 2450, 2000, 3000),
        )
        assert nxt == 15


class TestFindOptimalCrf:
    """Outer-loop integration: window checks, ceiling, floor, duplicate nudge."""

    def _make_windows(self, target=2500, allowed=500, tw=100, buf=50):
        return compute_windows(target, allowed, tw, buf)

    @mock.patch("reencode_av1.search._evaluate_crf_sample")
    def test_converges_in_confident_zone(self, mock_eval):
        # Use wider target window so confident zone has non-zero width.
        windows = self._make_windows(tw=400)
        # confident = [sample_lo+50? actually sample_confident_lo=2150, hi=2450].
        # 2300 lies inside.
        mock_eval.return_value = (2300, None, None)

        result = find_optimal_crf(
            Path("test.mp4"), windows, [], "128k", 3,
            max_iterations=15, crf_min=1, crf_max=57,
            crf_ceiling_fallback=48,
        )
        assert result.estimated_bitrate == 2300
        assert result.crf_ceiling_used is False
        assert mock_eval.call_count == 1

    @mock.patch("reencode_av1.search._evaluate_crf_sample")
    def test_max_crf_ceiling_uses_fallback(self, mock_eval):
        # Always overshoots → outer loop will hit max_crf with bitrate > target
        mock_eval.return_value = (9999, None, None)
        windows = self._make_windows()

        result = find_optimal_crf(
            Path("test.mp4"), windows, [], "128k", 3,
            max_iterations=15, crf_min=1, crf_max=57,
            crf_ceiling_fallback=48,
        )
        assert result.crf == 48
        assert result.crf_ceiling_used is True

    @mock.patch("reencode_av1.search._evaluate_crf_sample")
    def test_crf_min_floor_stops_search(self, mock_eval):
        # Always undershoots → outer loop hits crf_min with bitrate < accept_lo
        mock_eval.return_value = (10, None, None)
        windows = self._make_windows()

        result = find_optimal_crf(
            Path("test.mp4"), windows, [], "128k", 3,
            max_iterations=15, crf_min=5, crf_max=57,
            crf_ceiling_fallback=48,
        )
        assert result.crf == 5
        assert result.crf_ceiling_used is False

    @mock.patch("reencode_av1.search._evaluate_crf_sample")
    def test_returns_best_from_history_on_no_converge(self, mock_eval):
        # Bitrates always land in the accept window but never confident
        accept_hi = self._make_windows().sample_hi
        in_accept_not_confident = accept_hi - 1  # in accept, just below confident_hi but above confident_lo? actually sample_confident_hi == sample_hi. Use a value below sample_lo+something — instead just have it cycle.
        # Use a fixed value in accept but outside confident: with default windows
        # sample_lo=2050, sample_hi=2450, sample_confident_lo=2350, sample_confident_hi=2450
        # 2100 is in accept but outside confident.
        mock_eval.return_value = (2100, None, None)
        windows = self._make_windows()

        result = find_optimal_crf(
            Path("test.mp4"), windows, [], "128k", 3,
            max_iterations=3, crf_min=1, crf_max=57,
            crf_ceiling_fallback=48,
        )
        # Should return the in-range CRF, not the fallback
        assert result.estimated_bitrate == 2100
        assert result.crf_ceiling_used is False

    @mock.patch("reencode_av1.search._evaluate_crf_sample")
    def test_encode_failure_does_not_crash(self, mock_eval):
        mock_eval.return_value = (-1, None, None)
        windows = self._make_windows()

        result = find_optimal_crf(
            Path("test.mp4"), windows, [], "128k", 3,
            max_iterations=5, crf_min=1, crf_max=57,
            crf_ceiling_fallback=48,
        )
        # No history; outer loop falls back to crf_max
        assert result.crf == 57

    @mock.patch("reencode_av1.search._evaluate_crf_sample")
    def test_seeded_search_starts_near_seed(self, mock_eval):
        calls: list[int] = []

        def track(inp, crf, *a, **kw):
            calls.append(crf)
            return (2420, None, None)

        mock_eval.side_effect = track
        windows = self._make_windows()

        find_optimal_crf(
            Path("test.mp4"), windows, [], "128k", 3,
            max_iterations=15, crf_min=1, crf_max=57,
            crf_ceiling_fallback=48, seed_crf=30,
        )
        assert calls and abs(calls[0] - 30) <= 5

    @mock.patch("reencode_av1.search._evaluate_crf_sample")
    def test_keeps_winner_temp_file(self, mock_eval):
        with tempfile.NamedTemporaryFile(delete=False, suffix=".mkv") as f:
            f.write(b"\x00" * 100)
            tmp = Path(f.name)

        mock_eval.return_value = (2420, None, tmp)
        windows = self._make_windows()

        result = find_optimal_crf(
            Path("test.mp4"), windows, [], "128k", 3,
            max_iterations=15, crf_min=1, crf_max=57,
            crf_ceiling_fallback=48, full_encode=True,
        )
        assert result.temp_file == tmp
        assert tmp.exists()
        tmp.unlink(missing_ok=True)

    @mock.patch("reencode_av1.search._evaluate_crf_sample")
    def test_reuses_seed_temp_file_when_seed_selected(self, mock_eval):
        # A seeded point carrying a pre-made encode that the search settles on
        # is returned with that encode and is never re-evaluated.
        with tempfile.NamedTemporaryFile(delete=False, suffix=".mkv") as f:
            f.write(b"\x00" * 100)
            tmp = Path(f.name)

        windows = self._make_windows()
        no_probe = lambda *a, **kw: None  # search offers no further probes

        result = find_optimal_crf(
            Path("test.mp4"), windows, [], "128k", 3,
            max_iterations=15, crf_min=1, crf_max=57,
            crf_ceiling_fallback=48, search_method=no_probe,
            seed_known=[CrfPoint(31, 2100)], seed_temp_files={31: tmp},
        )
        assert result.crf == 31
        assert result.temp_file == tmp
        assert tmp.exists()
        assert mock_eval.call_count == 0
        tmp.unlink(missing_ok=True)

    @mock.patch("reencode_av1.search._evaluate_crf_sample")
    def test_cleans_up_non_winning_seed_temp_file(self, mock_eval):
        # The seed overshoots; the search's own encode converges and wins, so
        # the unused seed encode is deleted while the winner's is kept.
        with tempfile.NamedTemporaryFile(delete=False, suffix=".mkv") as f:
            f.write(b"\x00" * 100)
            seed_tmp = Path(f.name)
        with tempfile.NamedTemporaryFile(delete=False, suffix=".mkv") as f:
            f.write(b"\x00" * 100)
            winner_tmp = Path(f.name)

        # Wide window so 2300 lands in the confident zone and converges at once.
        windows = self._make_windows(tw=400)
        mock_eval.return_value = (2300, None, winner_tmp)
        always_forty = lambda *a, **kw: 40

        result = find_optimal_crf(
            Path("test.mp4"), windows, [], "128k", 3,
            max_iterations=15, crf_min=1, crf_max=57,
            crf_ceiling_fallback=48, search_method=always_forty,
            full_encode=True,
            seed_known=[CrfPoint(30, 9000)], seed_temp_files={30: seed_tmp},
        )
        assert result.crf == 40
        assert result.temp_file == winner_tmp
        assert winner_tmp.exists()
        assert not seed_tmp.exists()
        seed_tmp.unlink(missing_ok=True)
        winner_tmp.unlink(missing_ok=True)

    @mock.patch("reencode_av1.search._evaluate_crf_sample")
    def test_smart_method_converges(self, mock_eval):
        # Default method is smart search; verify it lands inside the window
        # against a log-linear bitrate model.
        def fake_encode(inp, crf, *a, **kw):
            return int(10000 * math.exp(-0.05 * crf)), None, None

        mock_eval.side_effect = fake_encode
        windows = self._make_windows()

        result = find_optimal_crf(
            Path("test.mp4"), windows, [], "128k", 3,
            max_iterations=15, crf_min=1, crf_max=57,
            crf_ceiling_fallback=48,
        )
        assert windows.sample_lo <= result.estimated_bitrate <= windows.sample_hi

    @mock.patch("reencode_av1.search._evaluate_crf_sample")
    def test_interpolation_method_converges(self, mock_eval):
        def fake_encode(inp, crf, *a, **kw):
            # Log-linear model: bitrate = 10000 * exp(-0.05 * crf)
            return int(10000 * math.exp(-0.05 * crf)), None, None

        mock_eval.side_effect = fake_encode
        windows = self._make_windows()

        result = find_optimal_crf(
            Path("test.mp4"), windows, [], "128k", 3,
            max_iterations=15, crf_min=1, crf_max=57,
            crf_ceiling_fallback=48,
            search_method=interpolation_next,
        )
        assert windows.sample_lo <= result.estimated_bitrate <= windows.sample_hi

    @mock.patch("reencode_av1.search._evaluate_crf_sample")
    def test_duplicate_crf_nudges_up_when_overshoot(self, mock_eval):
        # A search method that always returns 30 — the outer loop must nudge
        # the second call away from 30 (bitrate at 30 overshoots → step up).
        calls: list[int] = []

        def track(inp, crf, *a, **kw):
            calls.append(crf)
            # CRF 30 overshoots, CRF 31 in confident → second call wins.
            if crf == 30:
                return 5000, None, None
            return 2420, None, None

        mock_eval.side_effect = track

        always_thirty = lambda *a, **kw: 30
        windows = self._make_windows()

        result = find_optimal_crf(
            Path("test.mp4"), windows, [], "128k", 3,
            max_iterations=5, crf_min=1, crf_max=57,
            crf_ceiling_fallback=48, search_method=always_thirty,
        )
        assert calls == [30, 31]
        assert result.crf == 31

    @mock.patch("reencode_av1.search._evaluate_crf_sample")
    def test_duplicate_crf_stops_when_both_neighbours_tried(self, mock_eval):
        # Probe history forces (30 overshoots, 31 in range below confident).
        # Search method keeps suggesting 30 → outer loop nudges to 31 → 31
        # is already tried → stop and pick below-target one (CRF 31).
        windows = self._make_windows()
        seed_known = [CrfPoint(30, 5000), CrfPoint(31, 2100)]  # 2100 is in accept but not confident

        always_thirty = lambda *a, **kw: 30

        result = find_optimal_crf(
            Path("test.mp4"), windows, [], "128k", 3,
            max_iterations=5, crf_min=1, crf_max=57,
            crf_ceiling_fallback=48, search_method=always_thirty,
            seed_known=seed_known,
        )
        # Should pick CRF 31 (bitrate below target) without encoding
        assert result.crf == 31
        assert result.estimated_bitrate == 2100
        assert mock_eval.call_count == 0

    @mock.patch("reencode_av1.search._evaluate_crf_sample")
    def test_overhead_measured_only_on_first_encode(self, mock_eval):
        # Overhead is measured on the first encode of the phase, then cached:
        # later encodes are not asked to measure it again.
        measure_flags: list[bool] = []
        crfs = iter([20, 30, 40])

        def track(inp, crf, *a, **kw):
            measure_flags.append(kw.get("measure_overhead"))
            return (2100, 150, None)  # in accept, never confident → keeps searching

        mock_eval.side_effect = track
        windows = self._make_windows()

        find_optimal_crf(
            Path("test.mp4"), windows, [], "128k", 3,
            max_iterations=3, crf_min=1, crf_max=57,
            crf_ceiling_fallback=48,
            search_method=lambda history, ctx: next(crfs, None),
        )
        assert measure_flags == [True, False, False]

    @mock.patch("reencode_av1.search._evaluate_crf_sample")
    def test_seed_overhead_skips_measurement(self, mock_eval):
        # A caller-supplied overhead (the precise pass seeds it from the
        # pre-search full encode) means no encode is asked to measure it.
        measure_flags: list[bool] = []
        crfs = iter([20, 30])

        def track(inp, crf, *a, **kw):
            measure_flags.append(kw.get("measure_overhead"))
            return (2100, None, None)

        mock_eval.side_effect = track
        windows = self._make_windows()

        find_optimal_crf(
            Path("test.mp4"), windows, [], "128k", 3,
            max_iterations=2, crf_min=1, crf_max=57,
            crf_ceiling_fallback=48, seed_overhead=200,
            search_method=lambda history, ctx: next(crfs, None),
        )
        assert measure_flags and all(f is False for f in measure_flags)

    @mock.patch("reencode_av1.search._evaluate_crf_sample")
    def test_measured_overhead_reaches_search_method(self, mock_eval):
        # After the first encode measures overhead, the value is passed to the
        # search method on subsequent iterations via SearchContext.overhead.
        seen_overheads: list[int] = []
        crfs = iter([20, 30, 40])

        def track(inp, crf, *a, **kw):
            return (2100, 175, None)

        def method(history, ctx):
            seen_overheads.append(ctx.overhead)
            return next(crfs, None)

        mock_eval.side_effect = track
        windows = self._make_windows()

        find_optimal_crf(
            Path("test.mp4"), windows, [], "128k", 3,
            max_iterations=3, crf_min=1, crf_max=57,
            crf_ceiling_fallback=48, search_method=method,
        )
        # First call has no measurement yet (0); later calls see the measured 175.
        assert seen_overheads[0] == 0
        assert seen_overheads[1:] == [175, 175]


# ═══════════════════════════════════════════════════════════════════════════════
# __main__.py tests
# ═══════════════════════════════════════════════════════════════════════════════


class TestGetOutputPath:
    def test_mp4_to_mkv(self):
        assert get_output_path(Path("/dir/video.mp4")) == Path("/dir/video.mkv")

    def test_avi_to_mkv(self):
        assert get_output_path(Path("file.avi")) == Path("file.mkv")

    def test_mkv_gets_reencoded_suffix(self):
        assert get_output_path(Path("file.mkv")) == Path("file-reencoded.mkv")

    def test_mkv_case_insensitive(self):
        assert get_output_path(Path("file.MKV")) == Path("file-reencoded.MKV")

    def test_preserves_directory(self):
        result = get_output_path(Path("/a/b/c/video.mov"))
        assert result == Path("/a/b/c/video.mkv")


class TestComputeAudioBitrate:
    def test_auto_stereo(self):
        info = VideoInfo("h264", 1920, 1080, 30.0, False, 5000, 120.0, 3600, 2, "aac")
        s, k = compute_audio_bitrate(info, 0)
        assert s == "128k"
        assert k == 128

    def test_auto_mono(self):
        info = VideoInfo("h264", 1920, 1080, 30.0, False, 5000, 120.0, 3600, 1, "aac")
        s, k = compute_audio_bitrate(info, 0)
        assert s == "64k"
        assert k == 64

    def test_auto_surround(self):
        info = VideoInfo("h264", 1920, 1080, 30.0, False, 5000, 120.0, 3600, 6, "aac")
        s, k = compute_audio_bitrate(info, 0)
        assert s == "384k"
        assert k == 384

    def test_override(self):
        info = VideoInfo("h264", 1920, 1080, 30.0, False, 5000, 120.0, 3600, 2, "aac")
        s, k = compute_audio_bitrate(info, 96)
        assert s == "96k"
        assert k == 96


class TestValidateArgs:
    def _make_args(self, **overrides):
        defaults = {
            "target_bitrate": 2500,
            "accept_window": 1500,
            "confident_window": 200,
            "skip_below_bitrate": None,
            "sample_window_buffer": None,
            "crf_min": 1,
            "crf_max": 57,
            "crf_ceiling_fallback": None,
            "preset": 3,
            "segment_count": 5,
            "segment_duration": 3.0,
            "short_video_threshold": 90.0,
            "audio_bitrate": 0,
            "path": None,
            "scratch_dir": None,
            "precise": True,
            "precise_only": False,
        }
        defaults.update(overrides)
        return argparse.Namespace(**defaults)

    def test_valid_defaults(self):
        args = self._make_args()
        validate_args(args)  # should not raise
        assert args.sample_window_buffer == 50  # auto-resolved

    def test_negative_target_bitrate(self):
        args = self._make_args(target_bitrate=-1)
        with pytest.raises(SystemExit):
            validate_args(args)

    def test_window_exceeds_target(self):
        args = self._make_args(accept_window=3000)
        with pytest.raises(SystemExit):
            validate_args(args)

    def test_target_window_exceeds_allowed(self):
        args = self._make_args(confident_window=1600)
        with pytest.raises(SystemExit):
            validate_args(args)

    def test_crf_min_above_max(self):
        args = self._make_args(crf_min=50, crf_max=30)
        with pytest.raises(SystemExit):
            validate_args(args)

    def test_crf_out_of_range(self):
        args = self._make_args(crf_max=100)
        with pytest.raises(SystemExit):
            validate_args(args)

    def test_crf_min_equal_max_allowed(self):
        # Equal min/max pins a fixed CRF (search skipped); must be accepted.
        args = self._make_args(crf_min=30, crf_max=30)
        validate_args(args)  # should not raise

    def test_fixed_crf_allows_out_of_point_ceiling_fallback(self):
        # In fixed-CRF mode the ceiling can never be reached, so
        # --crf-ceiling-fallback is not constrained to the single [min, max] point.
        args = self._make_args(crf_min=30, crf_max=30, crf_ceiling_fallback=48)
        validate_args(args)  # should not raise

    def test_bad_preset(self):
        args = self._make_args(preset=20)
        with pytest.raises(SystemExit):
            validate_args(args)

    def test_zero_segment_count(self):
        args = self._make_args(segment_count=0)
        with pytest.raises(SystemExit):
            validate_args(args)

    def test_negative_segment_duration(self):
        args = self._make_args(segment_duration=-1.0)
        with pytest.raises(SystemExit):
            validate_args(args)

    def test_buffer_too_large(self):
        """Buffer so large that sample window has zero width."""
        args = self._make_args(sample_window_buffer=300)
        with pytest.raises(SystemExit):
            validate_args(args)

    def test_confident_zone_inverted(self):
        """confident_window < 2*buffer makes the confident zone inverted."""
        args = self._make_args(
            confident_window=10,
            sample_window_buffer=50,
        )
        with pytest.raises(SystemExit):
            validate_args(args)

    def test_precise_only_with_no_precise_is_rejected(self):
        # --precise-only (precise_only=True) and --no-precise (precise=False)
        # cannot be combined.
        args = self._make_args(precise_only=True, precise=False)
        with pytest.raises(SystemExit):
            validate_args(args)

    def test_precise_only_alone_is_allowed(self):
        args = self._make_args(precise_only=True, precise=True)
        validate_args(args)  # should not raise

    def test_valid_directory(self):
        with tempfile.TemporaryDirectory() as d:
            args = self._make_args(path=Path(d))
            validate_args(args)  # should not raise

    def test_valid_file(self):
        with tempfile.NamedTemporaryFile(suffix=".mp4") as f:
            args = self._make_args(path=Path(f.name))
            validate_args(args)  # should not raise

    def test_nonexistent_path(self):
        args = self._make_args(path=Path("/nonexistent/path/xyz"))
        with pytest.raises(SystemExit):
            validate_args(args)

    def test_none_path(self):
        args = self._make_args(path=None)
        validate_args(args)  # should not raise

    def test_scratch_dir_none(self):
        args = self._make_args(scratch_dir=None)
        validate_args(args)  # should not raise

    def test_scratch_dir_existing_directory(self):
        with tempfile.TemporaryDirectory() as d:
            args = self._make_args(scratch_dir=Path(d))
            validate_args(args)  # should not raise

    def test_scratch_dir_nonexistent_is_allowed(self):
        # A not-yet-existing scratch dir is fine; it is created at runtime.
        args = self._make_args(scratch_dir=Path("/nonexistent/scratch/xyz"))
        validate_args(args)  # should not raise

    def test_scratch_dir_is_a_file(self):
        with tempfile.NamedTemporaryFile(suffix=".tmp") as f:
            args = self._make_args(scratch_dir=Path(f.name))
            with pytest.raises(SystemExit):
                validate_args(args)


class TestBuildParser:
    def test_defaults(self):
        parser = build_parser()
        args = parser.parse_args([])
        assert args.target_bitrate == 2500
        assert args.preset == 3
        assert args.dry_run is False
        assert args.search_method == "smart"
        assert args.precise is True
        assert args.precise_only is False
        assert args.progress is True

    def test_no_precise_disables_precise(self):
        parser = build_parser()
        args = parser.parse_args(["--no-precise"])
        assert args.precise is False

    def test_no_progress_disables_progress(self):
        parser = build_parser()
        args = parser.parse_args(["--no-progress"])
        assert args.progress is False

    def test_precise_only_flag(self):
        parser = build_parser()
        args = parser.parse_args(["--precise-only"])
        assert args.precise_only is True
        assert args.precise is True

    def test_custom_values(self):
        parser = build_parser()
        args = parser.parse_args([
            "--target-bitrate", "2000",
            "--preset", "6",
            "--dry-run",
            "--search-method", "interpolation",
        ])
        assert args.target_bitrate == 2000
        assert args.preset == 6
        assert args.dry_run is True
        assert args.search_method == "interpolation"

    def test_search_method_binary(self):
        parser = build_parser()
        args = parser.parse_args(["--search-method", "binary"])
        assert args.search_method == "binary"

    def test_search_method_rejects_unknown(self):
        parser = build_parser()
        with pytest.raises(SystemExit):
            parser.parse_args(["--search-method", "bogus"])

    def test_default_path_is_none(self):
        parser = build_parser()
        args = parser.parse_args([])
        assert args.path is None

    def test_scratch_dir_default_is_none(self):
        parser = build_parser()
        args = parser.parse_args([])
        assert args.scratch_dir is None

    def test_scratch_dir_argument(self):
        parser = build_parser()
        args = parser.parse_args(["--scratch-dir", "/some/scratch"])
        assert args.scratch_dir == Path("/some/scratch")

    def test_path_argument(self):
        parser = build_parser()
        args = parser.parse_args(["/some/path"])
        assert args.path == Path("/some/path")

    def test_path_with_options(self):
        parser = build_parser()
        args = parser.parse_args(["--dry-run", "/some/path"])
        assert args.path == Path("/some/path")
        assert args.dry_run is True


class TestProcessFile:
    """Test process_file with mocked dependencies."""

    def _make_args(self, **overrides):
        defaults = {
            "target_bitrate": 2500,
            "skip_below_bitrate": 0,
            "accept_window": 1500,
            "confident_window": 100,
            "sample_window_buffer": 50,
            "crf_min": 1,
            "crf_max": 57,
            "crf_ceiling_fallback": 48,
            "preset": 3,
            "segment_count": 5,
            "segment_duration": 3.0,
            "short_video_threshold": 90.0,
            "audio_bitrate": 0,
            "search_method": "smart",
            "precise": False,
            "precise_only": False,
            "dry_run": False,
            "max_iterations": 15,
            "scratch_dir": None,
        }
        defaults.update(overrides)
        return argparse.Namespace(**defaults)

    @mock.patch("reencode_av1.__main__.get_video_info")
    def test_skips_when_no_info(self, mock_info):
        mock_info.return_value = None
        result = process_file(Path("test.mp4"), self._make_args())
        assert result.status.startswith("skipped")

    @mock.patch("reencode_av1.__main__.get_video_info")
    def test_skips_av1(self, mock_info):
        mock_info.return_value = VideoInfo(
            "av1", 1920, 1080, 30.0, False, 5000, 120.0, 3600, 2, "opus"
        )
        result = process_file(Path("test.mp4"), self._make_args())
        assert result.status.startswith("skipped")

    @mock.patch("reencode_av1.__main__.get_video_info")
    def test_skips_vp9(self, mock_info):
        mock_info.return_value = VideoInfo(
            "vp9", 1920, 1080, 30.0, False, 5000, 120.0, 3600, 2, "opus"
        )
        result = process_file(Path("test.mp4"), self._make_args())
        assert result.status.startswith("skipped")

    @mock.patch("reencode_av1.__main__.get_output_path")
    @mock.patch("reencode_av1.__main__.get_video_info")
    def test_skips_existing_output(self, mock_info, mock_output):
        mock_info.return_value = VideoInfo(
            "h264", 1920, 1080, 30.0, False, 5000, 120.0, 3600, 2, "aac"
        )
        # Create a real file so exists() returns True
        with tempfile.NamedTemporaryFile(delete=False, suffix=".mkv") as f:
            tmp = Path(f.name)
        try:
            mock_output.return_value = tmp
            result = process_file(Path("test.mp4"), self._make_args())
            assert result.status.startswith("skipped")
        finally:
            tmp.unlink(missing_ok=True)

    @mock.patch("reencode_av1.__main__.get_video_info")
    def test_dry_run(self, mock_info):
        mock_info.return_value = VideoInfo(
            "h264", 1920, 1080, 30.0, False, 5000, 120.0, 3600, 2, "aac"
        )
        result = process_file(Path("test.mp4"), self._make_args(dry_run=True))
        assert result.status.startswith("skipped")

    @mock.patch("reencode_av1.__main__.get_total_bitrate")
    @mock.patch("reencode_av1.__main__.encode_full")
    @mock.patch("reencode_av1.__main__.find_optimal_crf")
    @mock.patch("reencode_av1.__main__.get_output_path")
    @mock.patch("reencode_av1.__main__.get_video_info")
    def test_temp_file_cleanup_on_exception(
        self, mock_info, mock_output, mock_search, mock_encode, mock_bitrate
    ):
        """Temp files should be cleaned up even if an exception occurs."""
        mock_info.return_value = VideoInfo(
            "h264", 1920, 1080, 30.0, False, 5000, 120.0, 3600, 2, "aac"
        )
        # Output path that doesn't exist
        out_path = Path(tempfile.gettempdir()) / "nonexistent_output.mkv"
        mock_output.return_value = out_path

        # Create a temp file to simulate search result
        with tempfile.NamedTemporaryFile(delete=False, suffix=".mkv") as f:
            f.write(b"\x00" * 100)
            tmp = Path(f.name)

        mock_search.return_value = CrfResult(crf=30, estimated_bitrate=2400, temp_file=tmp)
        mock_encode.side_effect = RuntimeError("boom")

        with pytest.raises(RuntimeError):
            process_file(Path("test.mp4"), self._make_args())

        # Temp file should be cleaned up by the finally block
        assert not tmp.exists()

    @mock.patch("reencode_av1.__main__.get_total_bitrate")
    @mock.patch("reencode_av1.__main__.encode_full")
    @mock.patch("reencode_av1.__main__.find_optimal_crf")
    @mock.patch("reencode_av1.__main__.get_output_path")
    @mock.patch("reencode_av1.__main__.get_video_info")
    def test_scratch_dir_stages_source_and_moves_output(
        self, mock_info, mock_output, mock_search, mock_encode, mock_bitrate
    ):
        """With --scratch-dir, the source is staged locally, the encode reads from
        the staged copy, and the final output is moved to the destination."""
        mock_info.return_value = VideoInfo(
            "h264", 1920, 1080, 30.0, False, 5000, 120.0, 3600, 2, "aac"
        )
        mock_search.return_value = CrfResult(crf=30, estimated_bitrate=2400, temp_file=None)
        mock_bitrate.return_value = 2400

        with tempfile.TemporaryDirectory() as src_d, \
                tempfile.TemporaryDirectory() as dest_d, \
                tempfile.TemporaryDirectory() as scratch_d:
            source = Path(src_d) / "video.mp4"
            source.write_bytes(b"\x00" * 1024)
            dest_output = Path(dest_d) / "video.mkv"
            mock_output.return_value = dest_output
            scratch_dir = Path(scratch_d)

            encode_src = {}

            def _fake_encode(src, out_path, *a, **kw):
                encode_src["src"] = src
                Path(out_path).write_bytes(b"\x00" * 512)
                return 0

            mock_encode.side_effect = _fake_encode

            result = process_file(
                source, self._make_args(scratch_dir=scratch_dir)
            )

            assert result.status == "processed"
            # The final CRF chosen by the search is carried on the result.
            assert result.crf == 30
            # Encode read from the staged copy inside the scratch dir, not the source.
            assert encode_src["src"].parent == scratch_dir
            assert encode_src["src"] != source
            # Final output was moved to the destination.
            assert dest_output.exists()
            # No staged input/output left behind in the scratch dir.
            assert list(scratch_dir.iterdir()) == []

    @mock.patch("reencode_av1.__main__.get_total_bitrate")
    @mock.patch("reencode_av1.__main__.encode_full")
    @mock.patch("reencode_av1.__main__.find_optimal_crf")
    @mock.patch("reencode_av1.__main__.get_output_path")
    @mock.patch("reencode_av1.__main__.get_video_info")
    def test_precise_only_forces_full_search_on_long_video(
        self, mock_info, mock_output, mock_search, mock_encode, mock_bitrate
    ):
        """--precise-only runs a full-video search even for a video well above the
        short-video threshold: find_optimal_crf gets full_encode=True with no
        segment offsets, the search encode is reused, and encode_full is not called."""
        # 600s video — far above the 90s short-video threshold.
        mock_info.return_value = VideoInfo(
            "h264", 1920, 1080, 30.0, False, 5000, 600.0, 18000, 2, "aac"
        )
        mock_bitrate.return_value = 2400

        with tempfile.TemporaryDirectory() as dest_d:
            source = Path(dest_d) / "video.mp4"
            source.write_bytes(b"\x00" * 1024)
            dest_output = Path(dest_d) / "video.mkv"
            mock_output.return_value = dest_output

            # Search produces a usable full-video encode to be reused as output.
            search_encode = Path(dest_d) / "search-encode.mkv"
            search_encode.write_bytes(b"\x00" * 512)
            mock_search.return_value = CrfResult(
                crf=30, estimated_bitrate=2400, temp_file=search_encode
            )

            result = process_file(
                source, self._make_args(precise_only=True, precise=True)
            )

            assert result.status == "processed"
            # Full-video search: full_encode=True and no segment offsets.
            _, kwargs = mock_search.call_args
            assert kwargs["full_encode"] is True
            assert kwargs["offsets"] is None
            # The search encode was reused directly; no separate full encode ran.
            mock_encode.assert_not_called()
            assert dest_output.exists()

    @mock.patch("reencode_av1.__main__.get_total_bitrate")
    @mock.patch("reencode_av1.__main__.encode_full")
    @mock.patch("reencode_av1.__main__.find_optimal_crf")
    @mock.patch("reencode_av1.__main__.get_output_path")
    @mock.patch("reencode_av1.__main__.get_video_info")
    def test_scratch_dir_failure_leaves_no_files_and_no_output(
        self, mock_info, mock_output, mock_search, mock_encode, mock_bitrate
    ):
        """A failed encode must not move anything to the destination and must
        clean up the staged copies."""
        mock_info.return_value = VideoInfo(
            "h264", 1920, 1080, 30.0, False, 5000, 120.0, 3600, 2, "aac"
        )
        mock_search.return_value = CrfResult(crf=30, estimated_bitrate=2400, temp_file=None)
        mock_encode.return_value = 1  # non-zero exit -> failed

        with tempfile.TemporaryDirectory() as src_d, \
                tempfile.TemporaryDirectory() as dest_d, \
                tempfile.TemporaryDirectory() as scratch_d:
            source = Path(src_d) / "video.mp4"
            source.write_bytes(b"\x00" * 1024)
            dest_output = Path(dest_d) / "video.mkv"
            mock_output.return_value = dest_output
            scratch_dir = Path(scratch_d)

            result = process_file(
                source, self._make_args(scratch_dir=scratch_dir)
            )

            assert result.status == "failed"
            assert not dest_output.exists()
            assert list(scratch_dir.iterdir()) == []

    @mock.patch("reencode_av1.__main__.get_total_bitrate")
    @mock.patch("reencode_av1.__main__.encode_full")
    @mock.patch("reencode_av1.__main__.find_optimal_crf")
    @mock.patch("reencode_av1.__main__.get_output_path")
    @mock.patch("reencode_av1.__main__.get_video_info")
    def test_fixed_crf_skips_search_and_encodes_at_pinned_crf(
        self, mock_info, mock_output, mock_search, mock_encode, mock_bitrate
    ):
        """When --crf-min == --crf-max, no CRF search runs: the video is encoded
        once at that CRF (even on a long video that would otherwise sample)."""
        # 600s video — well above the short-video threshold, so a normal run
        # would sample and search. Fixed mode must bypass all of that.
        mock_info.return_value = VideoInfo(
            "h264", 1920, 1080, 30.0, False, 5000, 600.0, 18000, 2, "aac"
        )
        mock_bitrate.return_value = 9999  # bitrate is irrelevant in fixed mode

        with tempfile.TemporaryDirectory() as d:
            source = Path(d) / "video.mp4"
            source.write_bytes(b"\x00" * 1024)
            dest_output = Path(d) / "video.mkv"
            mock_output.return_value = dest_output

            def _fake_encode(src, out_path, crf, *a, **kw):
                Path(out_path).write_bytes(b"\x00" * 512)
                return 0

            mock_encode.side_effect = _fake_encode

            result = process_file(source, self._make_args(crf_min=30, crf_max=30))

            assert result.status == "processed"
            assert result.crf == 30
            # The search was never invoked.
            mock_search.assert_not_called()
            # A single full encode ran at the pinned CRF.
            mock_encode.assert_called_once()
            assert mock_encode.call_args.args[2] == 30
            assert dest_output.exists()

    @mock.patch("reencode_av1.__main__.get_total_bitrate")
    @mock.patch("reencode_av1.__main__.encode_full")
    @mock.patch("reencode_av1.__main__.find_optimal_crf")
    @mock.patch("reencode_av1.__main__.get_output_path")
    @mock.patch("reencode_av1.__main__.get_video_info")
    def test_fixed_crf_ignores_precise_mode_when_bitrate_out_of_range(
        self, mock_info, mock_output, mock_search, mock_encode, mock_bitrate
    ):
        """In fixed-CRF mode, an out-of-range output bitrate must NOT trigger the
        precise re-search, even with precise enabled."""
        mock_info.return_value = VideoInfo(
            "h264", 1920, 1080, 30.0, False, 5000, 600.0, 18000, 2, "aac"
        )
        mock_bitrate.return_value = 99999  # far above any window

        with tempfile.TemporaryDirectory() as d:
            source = Path(d) / "video.mp4"
            source.write_bytes(b"\x00" * 1024)
            dest_output = Path(d) / "video.mkv"
            mock_output.return_value = dest_output

            def _fake_encode(src, out_path, crf, *a, **kw):
                Path(out_path).write_bytes(b"\x00" * 512)
                return 0

            mock_encode.side_effect = _fake_encode

            result = process_file(
                source, self._make_args(crf_min=28, crf_max=28, precise=True)
            )

            assert result.status == "processed"
            assert result.crf == 28
            # No search at all — neither the initial search nor a precise re-search.
            mock_search.assert_not_called()
            mock_encode.assert_called_once()

    @mock.patch("reencode_av1.__main__.find_optimal_crf")
    @mock.patch("reencode_av1.__main__.get_video_info")
    def test_fixed_crf_still_skips_low_bitrate_source(self, mock_info, mock_search):
        """Fixed-CRF mode keeps the --skip-below-bitrate skip: a low-bitrate
        source is skipped before any encode."""
        mock_info.return_value = VideoInfo(
            "h264", 1920, 1080, 30.0, False, 800, 120.0, 3600, 2, "aac",
            total_bitrate_kbps=900,
        )
        result = process_file(
            Path("test.mp4"),
            self._make_args(crf_min=30, crf_max=30, skip_below_bitrate=2500),
        )
        assert result.status == "skipped:low_bitrate"
        mock_search.assert_not_called()

    @mock.patch("reencode_av1.__main__.get_total_bitrate")
    @mock.patch("reencode_av1.__main__.encode_full")
    @mock.patch("reencode_av1.__main__.find_optimal_crf")
    @mock.patch("reencode_av1.__main__.get_output_path")
    @mock.patch("reencode_av1.__main__.get_video_info")
    def test_fixed_crf_with_scratch_dir(
        self, mock_info, mock_output, mock_search, mock_encode, mock_bitrate
    ):
        """Fixed-CRF mode composes with --scratch-dir: the source is staged, the
        encode reads the staged copy at the pinned CRF, the output is moved to its
        destination, and nothing is left in the scratch dir."""
        mock_info.return_value = VideoInfo(
            "h264", 1920, 1080, 30.0, False, 5000, 600.0, 18000, 2, "aac"
        )
        mock_bitrate.return_value = 9999

        with tempfile.TemporaryDirectory() as src_d, \
                tempfile.TemporaryDirectory() as dest_d, \
                tempfile.TemporaryDirectory() as scratch_d:
            source = Path(src_d) / "video.mp4"
            source.write_bytes(b"\x00" * 1024)
            dest_output = Path(dest_d) / "video.mkv"
            mock_output.return_value = dest_output
            scratch_dir = Path(scratch_d)

            encode_src = {}

            def _fake_encode(src, out_path, crf, *a, **kw):
                encode_src["src"] = src
                encode_src["crf"] = crf
                Path(out_path).write_bytes(b"\x00" * 512)
                return 0

            mock_encode.side_effect = _fake_encode

            result = process_file(
                source,
                self._make_args(crf_min=30, crf_max=30, scratch_dir=scratch_dir),
            )

            assert result.status == "processed"
            assert result.crf == 30
            mock_search.assert_not_called()
            # Encode read the staged copy at the pinned CRF.
            assert encode_src["src"].parent == scratch_dir
            assert encode_src["crf"] == 30
            assert dest_output.exists()
            assert list(scratch_dir.iterdir()) == []


class TestProgress:
    """Test per-folder progress tracking."""

    def test_load_missing_returns_empty(self):
        with tempfile.TemporaryDirectory() as d:
            assert load_progress(Path(d)) == {}

    def test_record_then_load_roundtrip(self):
        with tempfile.TemporaryDirectory() as d:
            folder = Path(d)
            record_progress(folder, "a.mp4", "processed", 32)
            record_progress(folder, "b.mp4", "skipped:already_av1")
            data = load_progress(folder)
            assert data["a.mp4"] == {"status": "processed", "crf": 32}
            assert data["b.mp4"] == {"status": "skipped:already_av1"}
            # Written to the folder under the documented filename.
            assert (folder / PROGRESS_FILENAME).exists()
            assert progress_path_for(folder) == folder / PROGRESS_FILENAME

    def test_record_preserves_existing_entries(self):
        with tempfile.TemporaryDirectory() as d:
            folder = Path(d)
            record_progress(folder, "a.mp4", "processed", 30)
            # A second record re-reads the file and keeps the first entry.
            record_progress(folder, "c.mp4", "skipped:low_bitrate")
            data = load_progress(folder)
            assert set(data) == {"a.mp4", "c.mp4"}

    def test_record_overwrites_same_file(self):
        with tempfile.TemporaryDirectory() as d:
            folder = Path(d)
            record_progress(folder, "a.mp4", "failed")
            record_progress(folder, "a.mp4", "processed", 40)
            data = load_progress(folder)
            assert data["a.mp4"] == {"status": "processed", "crf": 40}

    def test_load_malformed_returns_empty(self):
        with tempfile.TemporaryDirectory() as d:
            folder = Path(d)
            (folder / PROGRESS_FILENAME).write_text("not json", encoding="utf-8")
            assert load_progress(folder) == {}

    def test_load_non_object_returns_empty(self):
        with tempfile.TemporaryDirectory() as d:
            folder = Path(d)
            (folder / PROGRESS_FILENAME).write_text("[1, 2, 3]", encoding="utf-8")
            assert load_progress(folder) == {}


# ═══════════════════════════════════════════════════════════════════════════════
# Integration-style: window invariants
# ═══════════════════════════════════════════════════════════════════════════════


class TestWindowInvariants:
    """Verify that window relationships hold across various parameter combos."""

    PARAMS = [
        (2500, 500, 100, 50),
        (1000, 300, 50, 25),
        (5000, 1000, 200, 100),
        (500, 200, 50, 10),
        (100, 50, 10, 5),
    ]

    @pytest.mark.parametrize("target,allowed,tw,buf", PARAMS)
    def test_final_hi_never_exceeds_target(self, target, allowed, tw, buf):
        w = compute_windows(target, allowed, tw, buf)
        assert w.final_hi == target
        assert w.final_accept_hi == target

    @pytest.mark.parametrize("target,allowed,tw,buf", PARAMS)
    def test_sample_hi_below_target(self, target, allowed, tw, buf):
        w = compute_windows(target, allowed, tw, buf)
        assert w.sample_hi <= target
        assert w.sample_confident_hi <= target

    @pytest.mark.parametrize("target,allowed,tw,buf", PARAMS)
    def test_no_negative_bounds(self, target, allowed, tw, buf):
        w = compute_windows(target, allowed, tw, buf)
        assert w.sample_lo >= 0
        assert w.sample_hi >= 0
        assert w.final_lo >= 0

    @pytest.mark.parametrize("target,allowed,tw,buf", PARAMS)
    def test_sample_narrower_than_final(self, target, allowed, tw, buf):
        w = compute_windows(target, allowed, tw, buf)
        assert w.sample_lo >= w.final_lo
        assert w.sample_hi <= w.final_hi

    @pytest.mark.parametrize("target,allowed,tw,buf", PARAMS)
    def test_confident_inside_sample(self, target, allowed, tw, buf):
        w = compute_windows(target, allowed, tw, buf)
        assert w.sample_confident_lo >= w.sample_lo
        assert w.sample_confident_hi <= w.sample_hi


if __name__ == "__main__":
    pytest.main([__file__, "-v"])