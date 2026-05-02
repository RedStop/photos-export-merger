"""CRF search strategies: binary search and log-linear interpolation."""

from __future__ import annotations

import logging
import math
from dataclasses import dataclass
from pathlib import Path

from .encode import encode_sample, encode_segments
from .filters import BitrateWindows

log = logging.getLogger(__name__)


@dataclass
class CrfResult:
    """Result of a CRF search."""

    crf: int
    estimated_bitrate: int
    temp_file: Path | None = None


@dataclass
class _KnownPoint:
    """A measured CRF data point collected during interpolation search."""
    crf: int
    bitrate: int
    temp_file: Path | None = None


@dataclass
class _SearchState:
    """Mutable state shared across search phases."""

    best_crf: int = -1
    best_bitrate: int = 0
    best_temp_file: Path | None = None
    total_iterations: int = 0

    # Proven bounds: tighten the range as we learn
    proven_too_high_crf: int = -1  # lowest CRF known to overshoot target
    proven_too_low_crf: int = -1   # highest CRF known to undershoot floor

    def update_best(
        self,
        crf: int,
        bitrate: int,
        temp_file: Path | None = None,
    ) -> None:
        """Replace the current best, cleaning up the previous temp file."""
        if self.best_temp_file and self.best_temp_file.exists():
            self.best_temp_file.unlink(missing_ok=True)
        self.best_crf = crf
        self.best_bitrate = bitrate
        self.best_temp_file = temp_file

    def clear_best(self) -> None:
        """Discard the current best (and its temp file)."""
        if self.best_temp_file and self.best_temp_file.exists():
            self.best_temp_file.unlink(missing_ok=True)
        self.best_crf = -1
        self.best_bitrate = 0
        self.best_temp_file = None


def _evaluate_crf_sample(
    input_path: Path,
    crf: int,
    extra_args: list[str],
    audio_bitrate: str,
    preset: int,
    audio_bitrate_kbps: int,
    *,
    offsets: list[float] | None,
    seg_duration: float,
    full_encode: bool,
    has_audio: bool = True,
) -> tuple[int, Path | None]:
    """Evaluate a CRF value using the appropriate sampling method.

    Returns ``(bitrate_kbps, temp_file_or_none)``.
    """
    if full_encode:
        return encode_sample(
            input_path, crf, extra_args, audio_bitrate, preset,
            audio_bitrate_kbps, keep_file=True, has_audio=has_audio,
        )
    if offsets:
        bitrate = encode_segments(
            input_path, crf, extra_args, audio_bitrate, preset,
            audio_bitrate_kbps, offsets, seg_duration, has_audio=has_audio,
        )
        return bitrate, None
    # Single-segment fallback (shouldn't normally happen)
    bitrate, _ = encode_sample(
        input_path, crf, extra_args, audio_bitrate, preset,
        audio_bitrate_kbps, duration=seg_duration, has_audio=has_audio,
    )
    return bitrate, None


def _binary_search_phase(
    input_path: Path,
    lo: int,
    hi: int,
    windows: BitrateWindows,
    extra_args: list[str],
    audio_bitrate: str,
    preset: int,
    audio_bitrate_kbps: int,
    max_iterations: int,
    state: _SearchState,
    *,
    offsets: list[float] | None,
    seg_duration: float,
    full_encode: bool,
    label: str,
    has_audio: bool = True,
) -> bool:
    """Run one phase of binary search.

    Updates *state* in place.  Returns True if an acceptable result was
    found (and we can stop searching).
    """
    accept_lo, accept_hi, confident_lo, confident_hi = _select_windows(windows, full_encode)

    log.info(
        "  Binary search (%s): target=%d, accept=[%d, %d], confident=[%d, %d]",
        label, windows.target, accept_lo, accept_hi, confident_lo, confident_hi,
    )

    while lo <= hi and state.total_iterations < max_iterations:
        state.total_iterations += 1
        mid = (lo + hi) // 2

        bitrate, temp_file = _evaluate_crf_sample(
            input_path, mid, extra_args, audio_bitrate, preset,
            audio_bitrate_kbps,
            offsets=offsets, seg_duration=seg_duration,
            full_encode=full_encode, has_audio=has_audio,
        )

        if bitrate < 0:
            log.warning(
                "  Iteration %d: CRF=%d -> encode failed",
                state.total_iterations, mid,
            )
            if temp_file and temp_file.exists():
                temp_file.unlink(missing_ok=True)
            # Move away from the failing CRF
            if (mid - lo) >= (hi - mid):
                hi = mid - 1
            else:
                lo = mid + 1
            continue

        log.info(
            "  Iteration %d: CRF=%d -> %d kbps",
            state.total_iterations, mid, bitrate,
        )

        if accept_lo <= bitrate <= accept_hi:
            # In acceptable range
            state.update_best(mid, bitrate, temp_file)

            if confident_lo <= bitrate <= confident_hi:
                log.info(
                    "  Early exit: %d kbps is in confident zone [%d, %d]",
                    bitrate, confident_lo, confident_hi,
                )
                return True

            # Try lower CRF (higher quality) while staying in range
            hi = mid - 1

        elif bitrate > accept_hi:
            # Bitrate too high -> increase CRF
            if temp_file and temp_file.exists():
                temp_file.unlink(missing_ok=True)
            lo = mid + 1
            if state.proven_too_high_crf < 0 or mid < state.proven_too_high_crf:
                state.proven_too_high_crf = mid

        else:
            # Bitrate too low -> decrease CRF
            if state.best_crf < 0 or mid < state.best_crf:
                state.update_best(mid, bitrate, temp_file)
            elif temp_file and temp_file.exists():
                temp_file.unlink(missing_ok=True)
            hi = mid - 1
            if state.proven_too_low_crf < 0 or mid > state.proven_too_low_crf:
                state.proven_too_low_crf = mid

    # Check if the best is in the acceptable range
    return (
        state.best_crf >= 0
        and accept_lo <= state.best_bitrate <= accept_hi
    )


def find_optimal_crf(
    input_path: Path,
    windows: BitrateWindows,
    extra_args: list[str],
    audio_bitrate: str,
    preset: int,
    audio_bitrate_kbps: int,
    max_iterations: int,
    crf_min: int,
    crf_max: int,
    *,
    offsets: list[float] | None = None,
    seg_duration: float = 3.0,
    full_encode: bool = False,
    seed_crf: int = -1,
    seed_lo: int = -1,
    seed_hi: int = -1,
    seed_known: list[tuple[int, int]] | None = None,
    has_audio: bool = True,
) -> CrfResult:
    """Find the optimal CRF via binary search.

    Args:
        input_path: source video.
        windows: pre-computed bitrate acceptance windows.
        extra_args: additional ffmpeg arguments.
        audio_bitrate: audio bitrate string (e.g. ``"128k"``).
        preset: SVT-AV1 preset.
        audio_bitrate_kbps: numeric audio bitrate for calculations.
        max_iterations: maximum total search iterations.
        crf_min: absolute minimum CRF.
        crf_max: absolute maximum CRF.
        offsets: segment offsets for multi-segment sampling (None to disable).
        seg_duration: duration of each segment.
        full_encode: if True, encode the full video each iteration.
        seed_crf: if >= 0, start with a narrow window around this value.
        seed_lo: optional lower bound for the seeded phase.
        seed_hi: optional upper bound for the seeded phase.

    Returns:
        A :class:`CrfResult` with the chosen CRF, estimated bitrate,
        and optionally a temp file (when *full_encode* is True).
    """
    state = _SearchState()

    # Pre-populate proven bounds from any seed_known data points so that
    # the binary search range is tighter from the very first iteration.
    if seed_known:
        if full_encode:
            accept_lo_ck = windows.final_lo
            accept_hi_ck = windows.final_hi
        else:
            accept_lo_ck = windows.sample_lo
            accept_hi_ck = windows.sample_hi
        for sk_crf, sk_bitrate in seed_known:
            log.info(
                "  Binary search seeded with known point: CRF=%d -> %d kbps",
                sk_crf, sk_bitrate,
            )
            if sk_bitrate > accept_hi_ck:
                # This CRF overshoots — proven too-high bound
                if state.proven_too_high_crf < 0 or sk_crf < state.proven_too_high_crf:
                    state.proven_too_high_crf = sk_crf
            elif sk_bitrate < accept_lo_ck:
                # This CRF undershoots — proven too-low bound
                if state.proven_too_low_crf < 0 or sk_crf > state.proven_too_low_crf:
                    state.proven_too_low_crf = sk_crf

    phases: list[tuple[int, int, str]] = []

    if seed_crf >= 0:
        s_lo = max(crf_min, seed_crf - 5)
        s_hi = min(crf_max, seed_crf + 5)
        if seed_lo >= 0:
            s_lo = max(s_lo, seed_lo)
        if seed_hi >= 0:
            s_hi = min(s_hi, seed_hi)
        phases.append((s_lo, s_hi, f"seeded CRF={seed_crf} [{s_lo}, {s_hi}]"))

    # Dynamic fallback phase — placeholder, computed after phase 1
    phases.append((-1, -1, "DYNAMIC"))

    for phase_lo, phase_hi, label in phases:
        if label == "DYNAMIC":
            # Compute expanded range from proven bounds
            lo = (
                state.proven_too_high_crf + 1
                if state.proven_too_high_crf >= 0
                else crf_min
            )
            hi = (
                state.proven_too_low_crf - 1
                if state.proven_too_low_crf >= 0
                else crf_max
            )
            lo = max(crf_min, lo)
            hi = min(crf_max, hi)

            if lo > hi:
                log.info("  Expanded range [%d, %d] is empty, skipping", lo, hi)
                continue

            label = (
                f"expanded [{lo}, {hi}] "
                f"(bounds: high@{state.proven_too_high_crf}, "
                f"low@{state.proven_too_low_crf})"
            )
        else:
            lo, hi = phase_lo, phase_hi

        found = _binary_search_phase(
            input_path, lo, hi, windows, extra_args, audio_bitrate,
            preset, audio_bitrate_kbps, max_iterations, state,
            offsets=offsets, seg_duration=seg_duration,
            full_encode=full_encode, label=label, has_audio=has_audio,
        )

        if found:
            break

        # If seeded phase didn't find a sweet-spot, expand — but keep
        # proven bounds and don't discard a best that may still be usable
        if label.startswith("seeded"):
            log.info("  No sweet-spot in seeded phase, expanding search...")
            state.clear_best()

    if state.best_crf < 0:
        log.warning("  Binary search did not converge, falling back to CRF %d", crf_max)
        state.best_crf = crf_max
        state.best_bitrate = 0
        if state.best_temp_file and state.best_temp_file.exists():
            state.best_temp_file.unlink(missing_ok=True)
            state.best_temp_file = None

    log.info(
        "  Selected CRF=%d (estimated %d kbps)", state.best_crf, state.best_bitrate
    )
    return CrfResult(
        crf=state.best_crf,
        estimated_bitrate=state.best_bitrate,
        temp_file=state.best_temp_file,
    )


# ── Shared helpers ───────────────────────────────────────────────────────────

def _select_windows(
    windows: BitrateWindows,
    full_encode: bool,
) -> tuple[int, int, int, int]:
    """Return ``(accept_lo, accept_hi, confident_lo, confident_hi)`` for the current mode."""
    if full_encode:
        return windows.final_lo, windows.final_hi, windows.final_accept_lo, windows.final_accept_hi
    return windows.sample_lo, windows.sample_hi, windows.sample_confident_lo, windows.sample_confident_hi


# ── Interpolation ────────────────────────────────────────────────────────────

def interpolate_crf(
    crf1: int,
    bitrate1: int,
    crf2: int,
    bitrate2: int,
    target_bitrate: int,
    crf_min: int,
    crf_max: int,
) -> int:
    """Estimate the CRF for a target bitrate using log-linear interpolation.

    The relationship between CRF and log(bitrate) is approximately linear,
    so we interpolate in log-space for a better estimate.

    Returns a CRF value clamped to ``[crf_min, crf_max]``.
    """
    if bitrate1 <= 0 or bitrate2 <= 0 or bitrate1 == bitrate2:
        return (crf1 + crf2) // 2

    log_b1 = math.log(bitrate1)
    log_b2 = math.log(bitrate2)
    log_target = math.log(target_bitrate)

    if log_b1 == log_b2:
        return (crf1 + crf2) // 2

    crf_est = crf1 + (crf2 - crf1) * (log_b1 - log_target) / (log_b1 - log_b2)
    return max(crf_min, min(crf_max, round(crf_est)))


def _extrapolate_crf(
    known: list[_KnownPoint],
    target_bitrate: int,
    crf_min: int,
    crf_max: int,
    *,
    direction: int,
    crf_nudge_size: int = 5,
) -> int:
    """Extrapolate a CRF when all known points are on the same side of the target.

    *direction* must be +1 (all overshooting → need a higher CRF) or -1 (all
    undershooting → need a lower CRF).  Uses the two extreme known points to
    fit the log-linear slope; if only one point exists a synthetic anchor is
    added at the boundary of the CRF range.
    """
    anchor_lo = min(known, key=lambda x: x.crf)
    anchor_hi = max(known, key=lambda x: x.crf)

    if anchor_lo is anchor_hi:
        p0 = known[0]
        if direction == 1:
            # Virtual anchor at crf_max with near-zero bitrate pushes slope upward.
            crf = interpolate_crf(p0.crf, p0.bitrate, crf_max, 1, target_bitrate, crf_min, crf_max)
        else:
            # Virtual anchor at crf_min with 10× bitrate pushes slope downward.
            crf = interpolate_crf(crf_min, p0.bitrate * 10, p0.crf, p0.bitrate, target_bitrate, crf_min, crf_max)
    else:
        crf = interpolate_crf(
            anchor_lo.crf, anchor_lo.bitrate,
            anchor_hi.crf, anchor_hi.bitrate,
            target_bitrate, crf_min, crf_max,
        )

    if direction == 1:
        max_tried = anchor_hi.crf
        if crf <= max_tried:
            crf = min(crf_max, max_tried + crf_nudge_size)
    else:
        min_tried = anchor_lo.crf
        if crf >= min_tried:
            crf = max(crf_min, min_tried - crf_nudge_size)

    return crf


def _nudge_crf(crf: int, tried_crfs: set[int], crf_min: int, crf_max: int) -> int | None:
    """Return *crf*, nudged by ±1 if already tried, or None if no untried neighbour exists."""
    if crf not in tried_crfs:
        return crf
    if crf + 1 <= crf_max and crf + 1 not in tried_crfs:
        return crf + 1
    if crf - 1 >= crf_min and crf - 1 not in tried_crfs:
        return crf - 1
    return None


def _resolve_next_crf(
    known: list[_KnownPoint],
    best_crf: int,
    best_bitrate: int,
    accept_lo: int,
    accept_hi: int,
    crf_min: int,
    crf_max: int,
) -> int | None:
    """Choose the next CRF to probe based on the current known-points table.

    accept_hi is also the target bitrate.

    Returns the suggested CRF integer, or None if the search should stop
    (no further improvement is possible).
    """
    above    = [p for p in known if p.bitrate > accept_hi]
    below    = [p for p in known if p.bitrate < accept_lo]
    in_range = [p for p in known if accept_lo <= p.bitrate <= accept_hi]
    target_bitrate = accept_hi

    if in_range:
        if not above:
            # No overshoot to bracket against, but the seed landed below the
            # target bitrate. There may still be a lower CRF (higher quality,
            # higher bitrate) that stays within accept_hi, so try to push
            # toward the top of the acceptable range.

            # Use the lowest-CRF in-range point as the reference
            lowest_crf_in_range = min(in_range, key=lambda x: x.crf)
            if lowest_crf_in_range.crf <= crf_min:
                # Already at the quality ceiling — nothing lower to try.
                log.info(
                    "  No above point and lowest in-range CRF=%d is already at "
                    "crf_min=%d — cannot lower CRF further, stopping",
                    lowest_crf_in_range.crf, crf_min,
                )
                return None

            # Extrapolate toward accept_hi from the lowest-CRF in-range point
            crf = _extrapolate_crf(known, target_bitrate, crf_min, crf_max, direction=-1, crf_nudge_size=2)

            # Clamp strictly below the current best (lower CRF = higher quality)
            # and above crf_min.
            crf = max(crf_min, min(lowest_crf_in_range.crf - 1, crf))

            log.info(
                "  No above point; probing lower CRF=%d to approach "
                "accept_hi=%d (best in-range: CRF=%d @ %d kbps)",
                crf, accept_hi, best_crf, best_bitrate,
            )
            return crf

        nearest_above = max(above, key=lambda x: x.crf)
        if best_crf - nearest_above.crf <= 1:
            log.info(
                "  Interpolation brackets are consecutive CRFs "
                "(above=%d @ %d kbps, best=%d @ %d kbps) — "
                "no integer CRF to probe, stopping",
                nearest_above.crf, nearest_above.bitrate,
                best_crf, best_bitrate,
            )
            return None

        crf = interpolate_crf(
            best_crf, best_bitrate,
            nearest_above.crf, nearest_above.bitrate,
            target_bitrate, crf_min, crf_max,
        )
        return max(nearest_above.crf + 1, min(best_crf - 1, crf))

    if above and below:
        nearest_above = max(above, key=lambda x: x.crf)
        nearest_below = min(below, key=lambda x: x.crf)

        if nearest_below.crf - nearest_above.crf <= 1:
            log.info(
                "  Interpolation brackets are consecutive CRFs "
                "(above=%d @ %d kbps, below=%d @ %d kbps) — "
                "no integer CRF to probe; returning nearest overshoot CRF=%d",
                nearest_above.crf, nearest_above.bitrate,
                nearest_below.crf, nearest_below.bitrate,
                nearest_above.crf,
            )
            return nearest_above  # type: ignore[return-value]  # sentinel: caller checks for _KnownPoint

        crf = interpolate_crf(
            nearest_below.crf, nearest_below.bitrate,
            nearest_above.crf, nearest_above.bitrate,
            target_bitrate, crf_min, crf_max,
        )
        return max(nearest_above.crf + 1, min(nearest_below.crf - 1, crf))

    if above:
        return _extrapolate_crf(known, target_bitrate, crf_min, crf_max, direction=1)

    if below:
        return _extrapolate_crf(known, target_bitrate, crf_min, crf_max, direction=-1)

    return None


def find_optimal_crf_interpolated(
    input_path: Path,
    windows: BitrateWindows,
    extra_args: list[str],
    audio_bitrate: str,
    preset: int,
    audio_bitrate_kbps: int,
    max_iterations: int,
    crf_min: int,
    crf_max: int,
    *,
    offsets: list[float] | None = None,
    seg_duration: float = 3.0,
    full_encode: bool = False,
    seed_crf: int = -1,
    seed_lo: int = -1,
    seed_hi: int = -1,
    seed_known: list[tuple[int, int]] | None = None,
    has_audio: bool = True,
) -> CrfResult:
    """Find optimal CRF using log-linear interpolation with binary search fallback.

    Probes two initial CRF values, interpolates, and refines.  Falls back
    to binary search if interpolation doesn't converge within a few steps.

    When *seed_crf* is provided, the initial probes are placed around the
    seed value for faster convergence.

    When *seed_known* is provided, those ``(crf, bitrate)`` pairs are
    injected into the known-points table before the first probe, so that
    data already collected (e.g. from a preceding full encode) immediately
    guides the interpolation rather than being discarded.  Points whose
    bitrate falls in the acceptable range also pre-populate *best_crf* /
    *best_bitrate*, potentially allowing the search to skip encodes
    entirely if the seeded result is already good enough.
    """
    accept_lo, accept_hi, confident_lo, confident_hi = _select_windows(windows, full_encode)

    known: list[_KnownPoint] = []
    best_crf = -1
    best_bitrate = 0
    tried_crfs: set[int] = set()

    def _cleanup_except(winner: Path | None) -> None:
        """Delete every temp file in *known* except *winner*."""
        for p in known:
            if p.temp_file and p.temp_file != winner and p.temp_file.exists():
                p.temp_file.unlink(missing_ok=True)

    # Inject any pre-existing measurements so interpolation starts with real data.
    if seed_known:
        for sk_crf, sk_bitrate in seed_known:
            log.info(
                "  Interpolation seeded with known point: CRF=%d -> %d kbps",
                sk_crf, sk_bitrate,
            )
            known.append(_KnownPoint(crf=sk_crf, bitrate=sk_bitrate))
            tried_crfs.add(sk_crf)
            if accept_lo <= sk_bitrate <= accept_hi:
                if best_crf < 0 or sk_crf < best_crf:
                    best_crf = sk_crf
                    best_bitrate = sk_bitrate

    # Build the list of initial CRF probes.
    if seed_crf >= 0:
        lo_bound = seed_lo if seed_lo >= 0 else crf_min
        hi_bound = seed_hi if seed_hi >= 0 else crf_max

        if seed_known:
            # One probe in the direction implied by the seeded measurement.
            sk_crf, sk_bitrate = seed_known[0]
            if sk_bitrate > accept_hi:
                probe_crfs = [min(hi_bound, sk_crf + 3)]
            elif sk_bitrate < accept_lo:
                probe_crfs = [max(lo_bound, sk_crf - 3)]
            else:
                probe_crfs = [sk_crf]
        else:
            probe_crfs = [
                max(lo_bound, seed_crf - 3),
                min(hi_bound, seed_crf + 3),
            ]
    else:
        # Probe at the 25th and 75th percentile of the empirical sweet-spot [24, 50]
        # to bracket the target quickly without wasting encodes at extreme CRF values.
        _SWEET_LO, _SWEET_HI = 24, 50
        sweet_lo = max(crf_min, _SWEET_LO)
        sweet_hi = min(crf_max, _SWEET_HI)
        probe_crfs = [
            sweet_lo + (sweet_hi - sweet_lo) // 4,      # ~30
            sweet_lo + 3 * (sweet_hi - sweet_lo) // 4,  # ~44
        ]

    interpolation_iters = max(max_iterations, 6)

    for iteration in range(1, interpolation_iters + 1):
        if iteration <= len(probe_crfs):
            crf = probe_crfs[iteration - 1]
        elif len(known) >= 2:
            # accept_hi is the target bitrate
            next_crf = _resolve_next_crf(
                known, best_crf, best_bitrate,
                accept_lo, accept_hi, crf_min, crf_max,
            )
            if next_crf is None:
                break
            # _resolve_next_crf returns a _KnownPoint as a sentinel when the
            # consecutive-bracket early-return case is hit.
            if isinstance(next_crf, _KnownPoint):
                winner = next_crf
                _cleanup_except(winner.temp_file)
                return CrfResult(
                    crf=winner.crf,
                    estimated_bitrate=winner.bitrate,
                    temp_file=winner.temp_file,
                )
            crf = next_crf
        else:
            break

        crf = max(crf_min, min(crf_max, crf))
        crf = _nudge_crf(crf, tried_crfs, crf_min, crf_max)
        if crf is None:
            break
        tried_crfs.add(crf)

        bitrate, temp_file = _evaluate_crf_sample(
            input_path, crf, extra_args, audio_bitrate, preset,
            audio_bitrate_kbps,
            offsets=offsets, seg_duration=seg_duration,
            full_encode=full_encode, has_audio=has_audio,
        )

        if bitrate < 0:
            log.warning("  Interpolation iter %d: CRF=%d -> encode failed", iteration, crf)
            if temp_file and temp_file.exists():
                temp_file.unlink(missing_ok=True)
            continue

        log.info("  Interpolation iter %d: CRF=%d -> %d kbps", iteration, crf, bitrate)
        known.append(_KnownPoint(crf=crf, bitrate=bitrate, temp_file=temp_file))

        if accept_lo <= bitrate <= accept_hi:
            if best_crf < 0 or crf < best_crf:
                best_crf = crf
                best_bitrate = bitrate
            if confident_lo <= bitrate <= confident_hi:
                log.info("  Interpolation converged in confident zone")
                break

    # Resolve the temp file for the winner, then clean up everything else.
    best_temp_file: Path | None = None
    if best_crf >= 0:
        for p in known:
            if p.crf == best_crf:
                best_temp_file = p.temp_file
                break
    _cleanup_except(best_temp_file)

    if best_crf < 0:
        log.warning("  Interpolation did not converge, falling back to binary search")
        return find_optimal_crf(
            input_path, windows, extra_args, audio_bitrate, preset,
            audio_bitrate_kbps, max_iterations, crf_min, crf_max,
            offsets=offsets, seg_duration=seg_duration,
            full_encode=full_encode,
            seed_crf=seed_crf, seed_lo=seed_lo, seed_hi=seed_hi,
            seed_known=seed_known, has_audio=has_audio,
        )

    log.info("  Selected CRF=%d (estimated %d kbps)", best_crf, best_bitrate)
    return CrfResult(crf=best_crf, estimated_bitrate=best_bitrate, temp_file=best_temp_file)