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
    get_video_bitrate,
    get_video_info,
)
from reencode_av1.encode import (
    _base_encode_args,
    _extract_vf_filter,
    _parse_time_to_seconds,
)
from reencode_av1.search import (
    CrfResult,
    _SearchState,
    interpolate_crf,
    find_optimal_crf,
    find_optimal_crf_interpolated,
)
from reencode_av1.__main__ import (
    build_parser,
    compute_audio_bitrate,
    get_output_path,
    validate_args,
    process_file,
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


class TestGetVideoBitrate:
    @mock.patch("reencode_av1.probe.run_ffprobe")
    def test_stream_bitrate(self, mock_probe):
        mock_probe.return_value = {
            "streams": [
                {"codec_type": "video", "bit_rate": "3000000"},
            ],
            "format": {"duration": "60"},
        }
        assert get_video_bitrate(Path("out.mkv"), 128) == 3000

    @mock.patch("reencode_av1.probe.run_ffprobe")
    def test_fallback_filesize(self, mock_probe):
        mock_probe.return_value = {
            "streams": [{"codec_type": "video"}],
            "format": {"duration": "10"},
        }
        # Create a real temp file so stat() works
        with tempfile.NamedTemporaryFile(suffix=".mkv", delete=False) as f:
            # Write 1,000,000 bytes
            f.write(b"\x00" * 1_000_000)
            tmp = Path(f.name)
        try:
            bitrate = get_video_bitrate(tmp, 128)
            # file_size=1000000 bytes, duration=10s, audio=128kbps
            # video_bits = (1000000*8) - (128*1000*10) = 8000000 - 1280000 = 6720000
            # video_kbps = 6720000 / 10 / 1000 = 672
            assert bitrate == 672
        finally:
            tmp.unlink(missing_ok=True)

    @mock.patch("reencode_av1.probe.run_ffprobe")
    def test_zero_duration(self, mock_probe):
        mock_probe.return_value = {
            "streams": [{"codec_type": "video"}],
            "format": {"duration": "0"},
        }
        assert get_video_bitrate(Path("out.mkv"), 128) == -1

    @mock.patch("reencode_av1.probe.run_ffprobe")
    def test_probe_failure(self, mock_probe):
        mock_probe.side_effect = json.JSONDecodeError("err", "", 0)
        assert get_video_bitrate(Path("out.mkv"), 128) == -1


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
        info = VideoInfo("h264", 1920, 1080, 30.0, False, 5000, 120.0, 2, "aac")
        args = build_extra_args(info)
        assert "-vf" not in args
        assert "-g" in args
        assert "-keyint_min" in args
        g_idx = args.index("-g")
        assert args[g_idx + 1] == "240"  # 30 * 8
        k_idx = args.index("-keyint_min")
        assert args[k_idx + 1] == "120"  # 30 * 4

    def test_4k_adds_scale(self):
        info = VideoInfo("h264", 3840, 2160, 30.0, False, 15000, 120.0, 2, "aac")
        args = build_extra_args(info)
        assert "-vf" in args
        vf_idx = args.index("-vf")
        assert args[vf_idx + 1] == "scale=-2:1080"

    def test_zero_fps_defaults_to_30(self):
        info = VideoInfo("h264", 1920, 1080, 0.0, False, 5000, 120.0, 2, "aac")
        args = build_extra_args(info)
        g_idx = args.index("-g")
        assert args[g_idx + 1] == "240"  # 30 * 8

    def test_60fps_gop(self):
        info = VideoInfo("h264", 1920, 1080, 60.0, False, 5000, 120.0, 2, "aac")
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
        # target=2500, allowed=500, target_window=100, buffer=50
        w = compute_windows(2500, 500, 100, 50)
        assert w.target == 2500
        assert w.sample_lo == 2050   # 2500 - 500 + 50
        assert w.sample_hi == 2450   # 2500 - 50
        assert w.sample_confident_lo == 2400  # 2500 - 100
        assert w.sample_confident_hi == 2450  # 2500 - 50
        assert w.final_lo == 2000    # 2500 - 500
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


class TestSearchState:
    def test_initial(self):
        s = _SearchState()
        assert s.best_crf == -1
        assert s.best_bitrate == 0
        assert s.best_temp_file is None

    def test_update_best(self):
        s = _SearchState()
        s.update_best(30, 2400)
        assert s.best_crf == 30
        assert s.best_bitrate == 2400

    def test_update_best_cleans_old_file(self):
        with tempfile.NamedTemporaryFile(delete=False, suffix=".mkv") as f:
            old_path = Path(f.name)
        s = _SearchState()
        s.update_best(30, 2400, old_path)
        assert old_path.exists()

        s.update_best(28, 2450)
        assert not old_path.exists()

    def test_clear_best(self):
        with tempfile.NamedTemporaryFile(delete=False, suffix=".mkv") as f:
            tmp = Path(f.name)
        s = _SearchState()
        s.update_best(30, 2400, tmp)
        s.clear_best()
        assert s.best_crf == -1
        assert not tmp.exists()


class TestInterpolateCrf:
    def test_basic_interpolation(self):
        # CRF 20 -> 5000 kbps, CRF 40 -> 1000 kbps, target 2500
        result = interpolate_crf(20, 5000, 40, 1000, 2500, 1, 63)
        assert 1 <= result <= 63
        # Should be between 20 and 40
        assert 20 <= result <= 40

    def test_clamped_to_min(self):
        # Target much higher than both points -> clamp to min
        result = interpolate_crf(20, 100, 40, 50, 50000, 5, 63)
        assert result == 5

    def test_clamped_to_max(self):
        # Target much lower than both points -> clamp to max
        result = interpolate_crf(20, 50000, 40, 30000, 10, 1, 63)
        assert result == 63

    def test_equal_bitrates_returns_midpoint(self):
        result = interpolate_crf(20, 2000, 40, 2000, 2500, 1, 63)
        assert result == 30

    def test_zero_bitrate_returns_midpoint(self):
        result = interpolate_crf(20, 0, 40, 2000, 2500, 1, 63)
        assert result == 30

    def test_log_linear_relationship(self):
        """Verify interpolation uses log-space, not linear."""
        # Two points: CRF 10 -> 10000, CRF 50 -> 100
        # Target: 1000 (geometric mean of 10000 and 100)
        # In log-space, 1000 is exactly at the midpoint of log(10000) and log(100)
        result = interpolate_crf(10, 10000, 50, 100, 1000, 1, 63)
        assert result == 30  # exact midpoint in log-space


class TestFindOptimalCrf:
    """Test binary search with mocked encoding."""

    def _make_windows(self, target=2500, allowed=500, tw=100, buf=50):
        return compute_windows(target, allowed, tw, buf)

    @mock.patch("reencode_av1.search._evaluate_crf_sample")
    def test_converges_in_confident_zone(self, mock_eval):
        """When a CRF immediately hits the confident zone, stop early."""
        mock_eval.return_value = (2420, None)  # always in confident zone
        windows = self._make_windows()

        result = find_optimal_crf(
            Path("test.mp4"), windows, [], "128k", 3, 128,
            max_iterations=15, crf_min=1, crf_max=63,
            seg_duration=3.0,
        )
        assert result.crf >= 1
        assert result.estimated_bitrate == 2420
        # Should converge in 1 iteration
        assert mock_eval.call_count == 1

    @mock.patch("reencode_av1.search._evaluate_crf_sample")
    def test_binary_search_narrows(self, mock_eval):
        """Search should call with decreasing range."""
        # Simulate: lower CRF -> higher bitrate
        def fake_encode(inp, crf, *a, **kw):
            # Simple linear model: bitrate = 5000 - crf * 80
            bitrate = 5000 - crf * 80
            return (bitrate, None)

        mock_eval.side_effect = fake_encode
        windows = self._make_windows()

        result = find_optimal_crf(
            Path("test.mp4"), windows, [], "128k", 3, 128,
            max_iterations=15, crf_min=1, crf_max=63,
            seg_duration=3.0,
        )
        assert result.crf >= 1
        assert result.estimated_bitrate > 0

    @mock.patch("reencode_av1.search._evaluate_crf_sample")
    def test_fallback_to_crf_max(self, mock_eval):
        """If nothing converges, fall back to crf_max."""
        mock_eval.return_value = (9999, None)  # always too high
        windows = self._make_windows()

        result = find_optimal_crf(
            Path("test.mp4"), windows, [], "128k", 3, 128,
            max_iterations=15, crf_min=1, crf_max=63,
            seg_duration=3.0,
        )
        assert result.crf == 63

    @mock.patch("reencode_av1.search._evaluate_crf_sample")
    def test_encode_failure_handled(self, mock_eval):
        """Failed encodes (bitrate=-1) shouldn't crash."""
        mock_eval.return_value = (-1, None)
        windows = self._make_windows()

        result = find_optimal_crf(
            Path("test.mp4"), windows, [], "128k", 3, 128,
            max_iterations=5, crf_min=1, crf_max=63,
            seg_duration=3.0,
        )
        # Should still return something (fallback to crf_max)
        assert result.crf == 63

    @mock.patch("reencode_av1.search._evaluate_crf_sample")
    def test_seeded_search(self, mock_eval):
        """Seeded search should start near the seed CRF."""
        calls = []

        def track_calls(inp, crf, *a, **kw):
            calls.append(crf)
            return (2420, None)  # confident zone

        mock_eval.side_effect = track_calls
        windows = self._make_windows()

        result = find_optimal_crf(
            Path("test.mp4"), windows, [], "128k", 3, 128,
            max_iterations=15, crf_min=1, crf_max=63,
            seg_duration=3.0, seed_crf=30,
        )
        # First call should be near seed CRF 30 (within ±5)
        assert 25 <= calls[0] <= 35

    @mock.patch("reencode_av1.search._evaluate_crf_sample")
    def test_keeps_best_temp_file(self, mock_eval):
        """When full_encode=True, the best temp file is preserved."""
        with tempfile.NamedTemporaryFile(delete=False, suffix=".mkv") as f:
            f.write(b"\x00" * 100)
            tmp = Path(f.name)

        mock_eval.return_value = (2420, tmp)  # confident zone with file
        windows = self._make_windows()

        result = find_optimal_crf(
            Path("test.mp4"), windows, [], "128k", 3, 128,
            max_iterations=15, crf_min=1, crf_max=63,
            seg_duration=3.0, full_encode=True,
        )
        assert result.temp_file == tmp
        assert tmp.exists()
        tmp.unlink(missing_ok=True)


class TestFindOptimalCrfInterpolated:
    """Test interpolation search with mocked encoding."""

    def _make_windows(self, target=2500, allowed=500, tw=100, buf=50):
        return compute_windows(target, allowed, tw, buf)

    @mock.patch("reencode_av1.search._evaluate_crf_sample")
    def test_converges(self, mock_eval):
        """Interpolation should find an acceptable CRF."""
        def fake_encode(inp, crf, *a, **kw):
            # Exponential model: bitrate = 10000 * exp(-0.05 * crf)
            bitrate = int(10000 * math.exp(-0.05 * crf))
            return (bitrate, None)

        mock_eval.side_effect = fake_encode
        windows = self._make_windows()

        result = find_optimal_crf_interpolated(
            Path("test.mp4"), windows, [], "128k", 3, 128,
            max_iterations=15, crf_min=1, crf_max=63,
            seg_duration=3.0,
        )
        assert result.crf >= 1
        assert windows.sample_lo <= result.estimated_bitrate <= windows.sample_hi

    @mock.patch("reencode_av1.search._evaluate_crf_sample")
    def test_falls_back_to_binary_search(self, mock_eval):
        """If interpolation fails, it should fall back to binary search."""
        call_count = [0]

        def failing_then_ok(inp, crf, *a, **kw):
            call_count[0] += 1
            # Always return out-of-range to force fallback, then eventually
            # return something in range for the binary search fallback
            if call_count[0] > 10:
                return (2420, None)
            return (9999, None)

        mock_eval.side_effect = failing_then_ok
        windows = self._make_windows()

        result = find_optimal_crf_interpolated(
            Path("test.mp4"), windows, [], "128k", 3, 128,
            max_iterations=15, crf_min=1, crf_max=63,
            seg_duration=3.0,
        )
        assert result.crf >= 1


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
        info = VideoInfo("h264", 1920, 1080, 30.0, False, 5000, 120.0, 2, "aac")
        s, k = compute_audio_bitrate(info, 0)
        assert s == "128k"
        assert k == 128

    def test_auto_mono(self):
        info = VideoInfo("h264", 1920, 1080, 30.0, False, 5000, 120.0, 1, "aac")
        s, k = compute_audio_bitrate(info, 0)
        assert s == "64k"
        assert k == 64

    def test_auto_surround(self):
        info = VideoInfo("h264", 1920, 1080, 30.0, False, 5000, 120.0, 6, "aac")
        s, k = compute_audio_bitrate(info, 0)
        assert s == "384k"
        assert k == 384

    def test_override(self):
        info = VideoInfo("h264", 1920, 1080, 30.0, False, 5000, 120.0, 2, "aac")
        s, k = compute_audio_bitrate(info, 96)
        assert s == "96k"
        assert k == 96


class TestValidateArgs:
    def _make_args(self, **overrides):
        defaults = {
            "target_bitrate": 2500,
            "allowed_bitrate_window": 500,
            "target_bitrate_window": 100,
            "sample_bitrate_window_buffer": None,
            "crf_min": 1,
            "crf_max": 63,
            "preset": 3,
            "segment_count": 5,
            "segment_duration": 3.0,
        }
        defaults.update(overrides)
        return argparse.Namespace(**defaults)

    def test_valid_defaults(self):
        args = self._make_args()
        validate_args(args)  # should not raise
        assert args.sample_bitrate_window_buffer == 50  # auto-resolved

    def test_negative_target_bitrate(self):
        args = self._make_args(target_bitrate=-1)
        with pytest.raises(SystemExit):
            validate_args(args)

    def test_window_exceeds_target(self):
        args = self._make_args(allowed_bitrate_window=3000)
        with pytest.raises(SystemExit):
            validate_args(args)

    def test_target_window_exceeds_allowed(self):
        args = self._make_args(target_bitrate_window=600)
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
        args = self._make_args(sample_bitrate_window_buffer=300)
        with pytest.raises(SystemExit):
            validate_args(args)

    def test_confident_zone_inverted(self):
        """target_window < buffer makes the confident zone inverted."""
        args = self._make_args(
            target_bitrate_window=10,
            sample_bitrate_window_buffer=50,
        )
        with pytest.raises(SystemExit):
            validate_args(args)


class TestBuildParser:
    def test_defaults(self):
        parser = build_parser()
        args = parser.parse_args([])
        assert args.target_bitrate == 2500
        assert args.preset == 3
        assert args.dry_run is False
        assert args.interpolate is False
        assert args.precise is False

    def test_custom_values(self):
        parser = build_parser()
        args = parser.parse_args([
            "--target-bitrate", "2000",
            "--preset", "6",
            "--dry-run",
            "--interpolate",
        ])
        assert args.target_bitrate == 2000
        assert args.preset == 6
        assert args.dry_run is True
        assert args.interpolate is True


class TestProcessFile:
    """Test process_file with mocked dependencies."""

    def _make_args(self, **overrides):
        defaults = {
            "target_bitrate": 2500,
            "allowed_bitrate_window": 500,
            "target_bitrate_window": 100,
            "sample_bitrate_window_buffer": 50,
            "crf_min": 1,
            "crf_max": 63,
            "preset": 3,
            "segment_count": 5,
            "segment_duration": 3.0,
            "audio_bitrate": 0,
            "interpolate": False,
            "precise": False,
            "dry_run": False,
            "max_iterations": 15,
        }
        defaults.update(overrides)
        return argparse.Namespace(**defaults)

    @mock.patch("reencode_av1.__main__.get_video_info")
    def test_skips_when_no_info(self, mock_info):
        mock_info.return_value = None
        result = process_file(Path("test.mp4"), self._make_args())
        assert result == "skipped"

    @mock.patch("reencode_av1.__main__.get_video_info")
    def test_skips_av1(self, mock_info):
        mock_info.return_value = VideoInfo(
            "av1", 1920, 1080, 30.0, False, 5000, 120.0, 2, "opus"
        )
        result = process_file(Path("test.mp4"), self._make_args())
        assert result == "skipped"

    @mock.patch("reencode_av1.__main__.get_video_info")
    def test_skips_vp9(self, mock_info):
        mock_info.return_value = VideoInfo(
            "vp9", 1920, 1080, 30.0, False, 5000, 120.0, 2, "opus"
        )
        result = process_file(Path("test.mp4"), self._make_args())
        assert result == "skipped"

    @mock.patch("reencode_av1.__main__.get_output_path")
    @mock.patch("reencode_av1.__main__.get_video_info")
    def test_skips_existing_output(self, mock_info, mock_output):
        mock_info.return_value = VideoInfo(
            "h264", 1920, 1080, 30.0, False, 5000, 120.0, 2, "aac"
        )
        # Create a real file so exists() returns True
        with tempfile.NamedTemporaryFile(delete=False, suffix=".mkv") as f:
            tmp = Path(f.name)
        try:
            mock_output.return_value = tmp
            result = process_file(Path("test.mp4"), self._make_args())
            assert result == "skipped"
        finally:
            tmp.unlink(missing_ok=True)

    @mock.patch("reencode_av1.__main__.get_video_info")
    def test_dry_run(self, mock_info):
        mock_info.return_value = VideoInfo(
            "h264", 1920, 1080, 30.0, False, 5000, 120.0, 2, "aac"
        )
        result = process_file(Path("test.mp4"), self._make_args(dry_run=True))
        assert result == "skipped"

    @mock.patch("reencode_av1.__main__.get_video_bitrate")
    @mock.patch("reencode_av1.__main__.encode_full")
    @mock.patch("reencode_av1.__main__.find_optimal_crf")
    @mock.patch("reencode_av1.__main__.get_output_path")
    @mock.patch("reencode_av1.__main__.get_video_info")
    def test_temp_file_cleanup_on_exception(
        self, mock_info, mock_output, mock_search, mock_encode, mock_bitrate
    ):
        """Temp files should be cleaned up even if an exception occurs."""
        mock_info.return_value = VideoInfo(
            "h264", 1920, 1080, 30.0, False, 5000, 120.0, 2, "aac"
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