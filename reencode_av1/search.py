"""CRF search strategies: binary search and log-linear interpolation."""

from __future__ import annotations

import logging
import math
from dataclasses import dataclass
from pathlib import Path

from .encode import encode_sample, encode_segments
from .filters import BitrateWindows

log = logging.getLogger(__name__)


# ── Public result type ────────────────────────────────────────────────────────

@dataclass
class CrfResult:
    """Result of a CRF search."""

    crf: int
    estimated_bitrate: int
    temp_file: Path | None = None
    crf_ceiling_used: bool = False


# ── Internal types ────────────────────────────────────────────────────────────

@dataclass
class _HistoryPoint:
    """A single measured CRF → bitrate data point."""
    crf: int
    bitrate: int
    temp_file: Path | None = None


# ── Window selector ───────────────────────────────────────────────────────────

def _select_windows(
    windows: BitrateWindows,
    full_encode: bool,
) -> tuple[int, int, int, int]:
    """Return ``(accept_lo, accept_hi, confident_lo, confident_hi)``."""
    if full_encode:
        return windows.final_lo, windows.final_hi, windows.final_accept_lo, windows.final_accept_hi
    return windows.sample_lo, windows.sample_hi, windows.sample_confident_lo, windows.sample_confident_hi


# ── Encode helper ─────────────────────────────────────────────────────────────

def _evaluate_crf(
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
    """Encode at *crf* and return ``(bitrate_kbps, temp_file_or_none)``."""
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
    # Single-segment fallback (short video without explicit offsets)
    bitrate, _ = encode_sample(
        input_path, crf, extra_args, audio_bitrate, preset,
        audio_bitrate_kbps, duration=seg_duration, has_audio=has_audio,
    )
    return bitrate, None


# ── Search methods ────────────────────────────────────────────────────────────
# Each method receives the full search history and returns the next CRF to try.
# They are pure selectors — they do not encode anything.
#
# Contract:
#   • Return value is clamped to [crf_min, crf_max].
#   • Duplicate-CRF detection, boundary checks, and stopping conditions are
#     handled by the outer loop; the method does not need to worry about those.

def _next_crf_binary(
    history: list[_HistoryPoint],
    target_bitrate: int,
    crf_min: int,
    crf_max: int,
    *,
    seed_crf: int = -1,
) -> int:
    """Binary search: return the next CRF to probe.

    When *seed_crf* is provided the first two probes are anchored to it:
      - iteration 0 (empty history): probe seed_crf itself.
      - iteration 1 (one point at seed_crf): probe seed_crf ± 5 in the
        direction that moves the bitrate toward the target.
    After those two seeded probes (or when no seed is given) the method
    derives [lo, hi] purely from the history and returns the midpoint.
    """
    # ── Seeded bootstrap (first two probes) ──────────────────────────────────
    if seed_crf >= 0:
        if not history:
            return max(crf_min, min(crf_max, seed_crf))

        if len(history) == 1 and history[0].crf == seed_crf:
            if history[0].bitrate > target_bitrate:
                # Bitrate too high at seed → need higher CRF (lower bitrate)
                return min(crf_max, seed_crf + 5)
            else:
                # Bitrate too low at seed → need lower CRF (higher bitrate)
                return max(crf_min, seed_crf - 5)

    # ── Normal binary search: derive [lo, hi] from history ───────────────────
    # lo  = one above the highest CRF whose bitrate overshot (too high bitrate).
    # hi  = one below the lowest  CRF whose bitrate undershot (too low bitrate).
    overshoot_crfs  = [p.crf for p in history if p.bitrate > target_bitrate]
    undershoot_crfs = [p.crf for p in history if p.bitrate <= target_bitrate]

    lo = (max(overshoot_crfs) + 1) if overshoot_crfs  else crf_min
    hi = (min(undershoot_crfs) - 1) if undershoot_crfs else crf_max

    lo = max(crf_min, lo)
    hi = min(crf_max, hi)

    if lo > hi:
        # History has pinned the range to nothing — return the undershoot
        # boundary (lower bitrate / higher CRF), the safe side.
        return max(crf_min, min(crf_max, hi + 1))

    return (lo + hi) // 2


def _next_crf_interpolated(
    history: list[_HistoryPoint],
    target_bitrate: int,
    crf_min: int,
    crf_max: int,
    *,
    seed_crf: int = -1,
) -> int:
    """Interpolation search: return the next CRF to probe.

    Uses log-linear interpolation between the nearest bracketing points.
    Falls back to extrapolation when all known points are on the same side
    of the target, and to a sweet-spot probe when fewer than two points exist.
    """
    # ── Bootstrap: no history yet ────────────────────────────────────────────
    if not history:
        if seed_crf >= 0:
            return max(crf_min, min(crf_max, seed_crf))
        # Probe at the 25th percentile of the empirical sweet-spot [24, 50]
        _SWEET_LO, _SWEET_HI = 24, 50
        sweet_lo = max(crf_min, _SWEET_LO)
        sweet_hi = min(crf_max, _SWEET_HI)
        return sweet_lo + (sweet_hi - sweet_lo) // 4  # ≈ 30

    # ── One point: pick the complementary probe ───────────────────────────────
    if len(history) == 1:
        p = history[0]
        if seed_crf >= 0 and p.crf == seed_crf:
            # Mirror the seed probe: go ±3 in the direction toward the target
            if p.bitrate > target_bitrate:
                return min(crf_max, seed_crf + 3)
            else:
                return max(crf_min, seed_crf - 3)
        # No seed, or seed not the only point: probe the complementary quartile
        _SWEET_LO, _SWEET_HI = 24, 50
        sweet_lo = max(crf_min, _SWEET_LO)
        sweet_hi = min(crf_max, _SWEET_HI)
        return sweet_lo + 3 * (sweet_hi - sweet_lo) // 4  # ≈ 44

    # ── Two or more points: interpolate / extrapolate ─────────────────────────
    above = [p for p in history if p.bitrate > target_bitrate]   # CRF too low
    below = [p for p in history if p.bitrate <= target_bitrate]  # CRF too high

    if above and below:
        # Bracket exists: interpolate between the nearest bracketing pair
        nearest_above = min(above, key=lambda p: p.bitrate)  # lowest overshoot
        nearest_below = max(below, key=lambda p: p.bitrate)  # highest undershoot
        crf = _interpolate_crf(
            nearest_below.crf, nearest_below.bitrate,
            nearest_above.crf, nearest_above.bitrate,
            target_bitrate, crf_min, crf_max,
        )
        # Keep strictly inside the bracket
        return max(nearest_above.crf + 1, min(nearest_below.crf - 1, crf))

    if above:
        # All points overshoot → extrapolate toward a higher CRF (lower bitrate)
        return _extrapolate_crf(history, target_bitrate, crf_min, crf_max, direction=1)

    # All points undershoot → extrapolate toward a lower CRF (higher bitrate)
    return _extrapolate_crf(history, target_bitrate, crf_min, crf_max, direction=-1)


# ── Interpolation math helpers ────────────────────────────────────────────────

def _interpolate_crf(
    crf1: int, bitrate1: int,
    crf2: int, bitrate2: int,
    target_bitrate: int,
    crf_min: int,
    crf_max: int,
) -> int:
    """Estimate the CRF for a target bitrate using log-linear interpolation.

    The relationship between CRF and log(bitrate) is approximately linear,
    so we interpolate in log-space for a better estimate.

    Returns a CRF value clamped to [crf_min, crf_max].
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
    history: list[_HistoryPoint],
    target_bitrate: int,
    crf_min: int,
    crf_max: int,
    *,
    direction: int,
    crf_nudge_size: int = 5,
) -> int:
    """Extrapolate a CRF when all known points are on one side of the target.

    *direction* is +1 (all overshoot → need higher CRF) or -1 (all undershoot
    → need lower CRF).  Uses the two extreme known points to fit the log-linear
    slope; if only one point exists a synthetic anchor is added at the boundary.
    """
    anchor_lo = min(history, key=lambda p: p.crf)
    anchor_hi = max(history, key=lambda p: p.crf)

    if anchor_lo is anchor_hi:
        p0 = history[0]
        if direction == 1:
            # All overshoot: virtual anchor at crf_max with near-zero bitrate
            # pushes the slope toward a higher CRF.
            crf = _interpolate_crf(p0.crf, p0.bitrate, crf_max, 1, target_bitrate, crf_min, crf_max)
        else:
            # All undershoot: virtual anchor at crf_min with 10× bitrate
            # pushes the slope toward a lower CRF.
            crf = _interpolate_crf(crf_min, p0.bitrate * 10, p0.crf, p0.bitrate, target_bitrate, crf_min, crf_max)
    else:
        crf = _interpolate_crf(
            anchor_lo.crf, anchor_lo.bitrate,
            anchor_hi.crf, anchor_hi.bitrate,
            target_bitrate, crf_min, crf_max,
        )

    if direction == 1:
        # Overshooting: nudge toward higher CRF if the estimate didn't move far enough
        max_tried = anchor_hi.crf
        if crf <= max_tried:
            crf = min(crf_max, max_tried + crf_nudge_size)
    else:
        # Undershooting: nudge toward lower CRF if the estimate didn't move far enough
        min_tried = anchor_lo.crf
        if crf >= min_tried:
            crf = max(crf_min, min_tried - crf_nudge_size)

    return crf


# ── Outer search loop ─────────────────────────────────────────────────────────

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
    seg_duration: float = 5.0,
    full_encode: bool = False,
    interpolate: bool = False,
    seed_crf: int = -1,
    # seed_known is reserved for future use where prior encode data (e.g. from a
    # preceding search pass) could be injected to guide the search without
    # re-encoding.  It is not currently used by the outer loop, but is kept here
    # so callers can pass it without API breakage when it becomes useful.
    seed_known: list[tuple[int, int]] | None = None,
    crf_ceiling_fallback: int = -1,
    has_audio: bool = True,
) -> CrfResult:
    """Find the optimal CRF using a unified search loop.

    The *interpolate* flag selects the CRF-selection strategy:
      - False (default): binary search
      - True: log-linear interpolation

    The outer loop drives all encode iterations and handles every stopping
    condition uniformly, regardless of strategy:

      1. Ask the search method for the next CRF to probe.
      2. If the suggested CRF is a duplicate (already in history), step ±1
         in the direction that moves the bitrate closer to the target.  If
         both the duplicate and its neighbour have been tried, stop and
         return the undershoot side (lower bitrate / higher CRF).
      3. Check for consecutive brackets: if the nearest overshoot and
         undershoot history points are adjacent CRF integers, return the
         undershoot side immediately.
      4. Encode at the suggested CRF and record the result.
      5. If the bitrate falls in the confident window → return immediately.
      6. If the bitrate falls in the accept window → record as best and
         continue searching for a lower CRF (higher quality) still in range.
      7. If CRF == crf_max and bitrate is still above accept_hi → the target
         is unachievable at acceptable quality; return with
         crf_ceiling_used=True at *crf_ceiling_fallback* (or crf_max).
      8. If CRF == crf_min and bitrate is still below accept_lo → return
         crf_min (cannot raise quality further).
      9. Otherwise continue to the next iteration.

    The target bitrate is always the upper edge of the active acceptance
    window (accept_hi), which is inferred from *full_encode* and *windows*:
    no separate target-bitrate parameter is required.
    """
    accept_lo, accept_hi, confident_lo, confident_hi = _select_windows(windows, full_encode)

    # The target bitrate is the upper edge of whichever window is active.
    # All search methods drive the bitrate toward this value.
    target_bitrate = accept_hi

    _next_crf_fn = _next_crf_interpolated if interpolate else _next_crf_binary

    method_label = "interpolation" if interpolate else "binary"
    log.info(
        "  CRF search (%s, %s): target=%d, accept=[%d, %d], confident=[%d, %d]",
        method_label, "full" if full_encode else "sample",
        target_bitrate, accept_lo, accept_hi, confident_lo, confident_hi,
    )

    history: list[_HistoryPoint] = []
    tried_crfs: set[int] = set()

    # Best result seen so far that is within [accept_lo, accept_hi].
    # We prefer the lowest CRF (highest quality) in that range.
    best: _HistoryPoint | None = None

    def _cleanup_except(keep: Path | None) -> None:
        """Delete every temp file in history except *keep*."""
        for p in history:
            if p.temp_file and p.temp_file != keep and p.temp_file.exists():
                p.temp_file.unlink(missing_ok=True)

    for iteration in range(1, max_iterations + 1):

        # ── Ask the search method for the next CRF ────────────────────────────
        crf = _next_crf_fn(
            history, target_bitrate, crf_min, crf_max,
            seed_crf=seed_crf,
        )
        crf = max(crf_min, min(crf_max, crf))

        # ── Duplicate detection ───────────────────────────────────────────────
        if crf in tried_crfs:
            # Look up this CRF in history to determine step direction.
            existing = next(p for p in history if p.crf == crf)
            if existing.bitrate > target_bitrate:
                # Bitrate too high → step up (higher CRF lowers bitrate)
                neighbour = crf + 1
            else:
                # Bitrate too low → step down (lower CRF raises bitrate)
                neighbour = crf - 1

            neighbour = max(crf_min, min(crf_max, neighbour))

            if neighbour in tried_crfs:
                # Both the suggested CRF and its natural neighbour have been
                # tried.  No valid integer CRF remains to probe — stop and
                # return the undershoot side (bitrate ≤ target / higher CRF).
                log.warning(
                    "  Iteration %d: CRF=%d already tried and neighbour CRF=%d also tried "
                    "— no valid CRF to probe, stopping search",
                    iteration, crf, neighbour,
                )
                undershoot = next(
                    (p for p in sorted(history, key=lambda p: p.crf, reverse=True)
                     if p.bitrate <= target_bitrate),
                    None,
                )
                if undershoot is None:
                    # All points overshoot; return the highest-CRF point (lowest bitrate)
                    undershoot = max(history, key=lambda p: p.crf)
                _cleanup_except(undershoot.temp_file)
                return CrfResult(
                    crf=undershoot.crf,
                    estimated_bitrate=undershoot.bitrate,
                    temp_file=undershoot.temp_file,
                )

            log.debug(
                "  Iteration %d: CRF=%d already tried, stepping to CRF=%d",
                iteration, crf, neighbour,
            )
            crf = neighbour

        # ── Consecutive-bracket check ─────────────────────────────────────────
        # If the nearest overshoot and nearest undershoot are adjacent integers
        # there is no CRF left to probe between them — stop early.
        overshoots  = [p for p in history if p.bitrate > target_bitrate]
        undershoots = [p for p in history if p.bitrate <= target_bitrate]
        if overshoots and undershoots:
            # Nearest overshoot: lowest CRF that still exceeds the target bitrate.
            # Nearest undershoot: lowest CRF (highest quality) that fits.
            nearest_over  = min(overshoots,  key=lambda p: p.crf)
            nearest_under = min(undershoots, key=lambda p: p.crf)
            if nearest_under.crf - nearest_over.crf <= 1:
                log.info(
                    "  Consecutive brackets: CRF=%d (%d kbps) and CRF=%d (%d kbps) "
                    "— no integer CRF to probe, stopping",
                    nearest_over.crf, nearest_over.bitrate,
                    nearest_under.crf, nearest_under.bitrate,
                )
                # Return the undershoot (bitrate ≤ target = lower bitrate = higher CRF)
                winner = nearest_under
                _cleanup_except(winner.temp_file)
                return CrfResult(
                    crf=winner.crf,
                    estimated_bitrate=winner.bitrate,
                    temp_file=winner.temp_file,
                )

        # ── Encode ────────────────────────────────────────────────────────────
        tried_crfs.add(crf)
        bitrate, temp_file = _evaluate_crf(
            input_path, crf, extra_args, audio_bitrate, preset,
            audio_bitrate_kbps,
            offsets=offsets, seg_duration=seg_duration,
            full_encode=full_encode, has_audio=has_audio,
        )

        if bitrate < 0:
            log.warning(
                "  Iteration %d: CRF=%d -> encode failed, skipping",
                iteration, crf,
            )
            if temp_file and temp_file.exists():
                temp_file.unlink(missing_ok=True)
            continue

        log.info("  Iteration %d: CRF=%d -> %d kbps", iteration, crf, bitrate)
        point = _HistoryPoint(crf=crf, bitrate=bitrate, temp_file=temp_file)
        history.append(point)

        # ── Acceptance check ──────────────────────────────────────────────────
        if accept_lo <= bitrate <= accept_hi:
            # Within the acceptance window — prefer lowest CRF (highest quality).
            if best is None or crf < best.crf:
                if best and best.temp_file and best.temp_file != temp_file:
                    best.temp_file.unlink(missing_ok=True)
                best = point

            if confident_lo <= bitrate <= confident_hi:
                log.info(
                    "  CRF=%d (%d kbps) is in confident zone [%d, %d] — stopping",
                    crf, bitrate, confident_lo, confident_hi,
                )
                _cleanup_except(best.temp_file)
                return CrfResult(
                    crf=best.crf,
                    estimated_bitrate=best.bitrate,
                    temp_file=best.temp_file,
                )

            # Acceptable but not confident — continue searching for a lower CRF
            # (higher quality) that is still in range.
            continue

        # ── CRF ceiling check ─────────────────────────────────────────────────
        if crf >= crf_max and bitrate > accept_hi:
            _fallback = crf_ceiling_fallback if crf_ceiling_fallback >= 0 else crf_max
            log.info(
                "  CRF=%d is at crf_max=%d and bitrate (%d kbps) still exceeds "
                "accept_hi (%d kbps) — target unachievable, using crf-ceiling-fallback=%d",
                crf, crf_max, bitrate, accept_hi, _fallback,
            )
            _cleanup_except(None)
            return CrfResult(
                crf=_fallback,
                estimated_bitrate=bitrate,
                temp_file=None,
                crf_ceiling_used=True,
            )

        # ── CRF floor check ───────────────────────────────────────────────────
        if crf <= crf_min and bitrate < accept_lo:
            log.info(
                "  CRF=%d is at crf_min=%d and bitrate (%d kbps) is still "
                "below accept_lo (%d kbps) — cannot raise quality further, using crf_min",
                crf, crf_min, bitrate, accept_lo,
            )
            _cleanup_except(point.temp_file)
            return CrfResult(
                crf=crf_min,
                estimated_bitrate=bitrate,
                temp_file=temp_file,
            )

        # ── Out of window — continue to next iteration ────────────────────────

    # ── Max iterations reached ────────────────────────────────────────────────
    if best is not None:
        log.info(
            "  Max iterations reached. Best in-window result: CRF=%d (%d kbps)",
            best.crf, best.bitrate,
        )
        _cleanup_except(best.temp_file)
        return CrfResult(crf=best.crf, estimated_bitrate=best.bitrate, temp_file=best.temp_file)

    # No in-window result — return the best undershoot (bitrate ≤ target) as the
    # safe side (lower bitrate is preferable to exceeding the target).
    undershoot_pts = [p for p in history if p.bitrate <= target_bitrate]
    if undershoot_pts:
        fallback = max(undershoot_pts, key=lambda p: p.bitrate)  # closest to target
        log.warning(
            "  Search did not converge. Best undershoot: CRF=%d (%d kbps)",
            fallback.crf, fallback.bitrate,
        )
        _cleanup_except(fallback.temp_file)
        return CrfResult(crf=fallback.crf, estimated_bitrate=fallback.bitrate, temp_file=fallback.temp_file)

    # Every encode overshot — return the highest-CRF point (lowest bitrate achieved).
    fallback = max(history, key=lambda p: p.crf)
    log.warning(
        "  Search did not converge and all encodes overshot. "
        "Using highest CRF tried: CRF=%d (%d kbps)",
        fallback.crf, fallback.bitrate,
    )
    _cleanup_except(fallback.temp_file)
    return CrfResult(crf=fallback.crf, estimated_bitrate=fallback.bitrate, temp_file=fallback.temp_file)