"""CRF search: outer loop with pluggable stateless search methods.

The outer ``find_optimal_crf`` function manages all state (history, temp
files, accept/confident checks, max-crf ceiling fallback, crf-min floor)
and delegates the choice of *which* CRF to probe next to a stateless
search method.  Each search method receives the full history of measured
``CrfPoint`` results plus the bounds and target windows, and returns
either an integer CRF to probe next, or ``None`` to stop searching.

Two search methods are provided: :func:`binary_search_next` and
:func:`interpolation_next`.  Adding a new strategy is a matter of writing
another function with the same signature.
"""

from __future__ import annotations

import logging
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

from .encode import encode_sample, encode_segments
from .filters import BitrateWindows

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class CrfPoint:
    """A measured point in the CRF search: a CRF and the bitrate it produced.

    CRF and bitrate move in opposite directions (higher CRF → lower
    bitrate), so pairing them in a named type avoids the ambiguity of a
    bare ``(int, int)`` tuple where it's easy to lose track of which is
    which.
    """

    crf: int
    bitrate: int


@dataclass
class CrfResult:
    """Result of a CRF search."""

    crf: int
    estimated_bitrate: int
    temp_file: Path | None = None
    crf_ceiling_used: bool = False


# A stateless search method.  Returns the next CRF to probe, or None to stop.
SearchMethod = Callable[
    [list[CrfPoint], int, int, int, int, int],
    "int | None",
]


# ── Sampling helper ─────────────────────────────────────────────────────────

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


def _select_windows(
    windows: BitrateWindows,
    full_encode: bool,
) -> tuple[int, int, int, int]:
    """Return ``(accept_lo, accept_hi, confident_lo, confident_hi)``."""
    if full_encode:
        return (
            windows.final_lo, windows.final_hi,
            windows.final_accept_lo, windows.final_accept_hi,
        )
    return (
        windows.sample_lo, windows.sample_hi,
        windows.sample_confident_lo, windows.sample_confident_hi,
    )


# ── Log-linear interpolation primitive ──────────────────────────────────────

def interpolate_crf(
    p1: CrfPoint,
    p2: CrfPoint,
    target_bitrate: int,
    crf_min: int,
    crf_max: int,
) -> int:
    """Estimate the CRF for a target bitrate using log-linear interpolation.

    The relationship between CRF and ``log(bitrate)`` is approximately
    linear, so interpolating in log-space yields a much better estimate
    than a linear fit.  The result is clamped to ``[crf_min, crf_max]``.
    """
    if p1.bitrate <= 0 or p2.bitrate <= 0 or p1.bitrate == p2.bitrate:
        return (p1.crf + p2.crf) // 2

    log_b1 = math.log(p1.bitrate)
    log_b2 = math.log(p2.bitrate)
    log_target = math.log(target_bitrate)

    if log_b1 == log_b2:
        return (p1.crf + p2.crf) // 2

    crf_est = p1.crf + (p2.crf - p1.crf) * (log_b1 - log_target) / (log_b1 - log_b2)
    return max(crf_min, min(crf_max, round(crf_est)))


def _extrapolate_crf(
    history: list[CrfPoint],
    target_bitrate: int,
    crf_min: int,
    crf_max: int,
    *,
    direction: int,
    crf_nudge_size: int = 5,
) -> int:
    """Extrapolate a CRF when all known points are on one side of the target.

    *direction* must be +1 (all overshooting → need higher CRF) or -1 (all
    undershooting → need lower CRF).
    """
    anchor_lo = min(history, key=lambda p: p.crf)
    anchor_hi = max(history, key=lambda p: p.crf)

    if anchor_lo.crf == anchor_hi.crf:
        point = history[0]
        if direction == 1:
            # Virtual anchor at crf_max with near-zero bitrate to push higher
            crf = interpolate_crf(point, CrfPoint(crf_max, 1), target_bitrate, crf_min, crf_max)
        else:
            # Virtual anchor at crf_min with 10x bitrate to push lower
            crf = interpolate_crf(CrfPoint(crf_min, point.bitrate * 10), point, target_bitrate, crf_min, crf_max)
    else:
        crf = interpolate_crf(anchor_lo, anchor_hi, target_bitrate, crf_min, crf_max)

    # If the estimate didn't move past the extreme tried point, nudge it.
    if direction == 1:
        if crf <= anchor_hi.crf:
            crf = min(crf_max, anchor_hi.crf + crf_nudge_size)
    else:
        if crf >= anchor_lo.crf:
            crf = max(crf_min, anchor_lo.crf - crf_nudge_size)

    return crf


# ── Stateless search methods ────────────────────────────────────────────────

def binary_search_next(
    history: list[CrfPoint],
    crf_min: int,
    crf_max: int,
    accept_lo: int,
    accept_hi: int,
    seed_crf: int = -1,
) -> int | None:
    """Choose the next CRF via binary search over proven bounds.

    Derives the current ``[lo, hi]`` range from the history:
      * The lowest CRF whose bitrate overshoots ``accept_hi`` becomes a
        strict lower bound on the next probe (lo = that CRF + 1).
      * The highest CRF whose bitrate undershoots ``accept_lo`` (or the
        lowest in-range CRF, when one exists) becomes a strict upper
        bound (hi = that CRF − 1).

    Returns the midpoint of the resulting range, or ``None`` if the range
    is empty.  When the history is empty, *seed_crf* (if in range) is
    used as the first probe; otherwise the midpoint of ``[crf_min, crf_max]``.
    """
    overshoot_crfs = [p.crf for p in history if p.bitrate > accept_hi]
    undershoot_crfs = [p.crf for p in history if p.bitrate < accept_lo]
    in_range_crfs = [p.crf for p in history if accept_lo <= p.bitrate <= accept_hi]

    lo = (max(overshoot_crfs) + 1) if overshoot_crfs else crf_min
    if in_range_crfs:
        # Try lower CRFs (higher quality) than the best in-range point.
        hi = min(in_range_crfs) - 1
    elif undershoot_crfs:
        hi = min(undershoot_crfs) - 1
    else:
        hi = crf_max

    lo = max(crf_min, lo)
    hi = min(crf_max, hi)

    if lo > hi:
        return None

    if not history and seed_crf >= 0 and lo <= seed_crf <= hi:
        return seed_crf

    return (lo + hi) // 2


def interpolation_next(
    history: list[CrfPoint],
    crf_min: int,
    crf_max: int,
    accept_lo: int,
    accept_hi: int,
    seed_crf: int = -1,
) -> int | None:
    """Choose the next CRF via log-linear interpolation.

    With < 2 history points, returns probe CRFs designed to bracket the
    target quickly.  With 2+ points, interpolates between bracketing
    points or extrapolates when all points are on one side.  Returns
    ``None`` when no useful integer CRF remains to probe (e.g. the
    bracketing points are consecutive integers).
    """
    target_bitrate = accept_hi

    if len(history) == 0:
        if seed_crf >= 0:
            return max(crf_min, min(crf_max, seed_crf - 3))
        # Probe near the 25th percentile of the empirical sweet-spot [24, 50].
        return max(crf_min, min(crf_max, 30))

    if len(history) == 1:
        point = history[0]
        if point.bitrate > accept_hi:
            return min(crf_max, point.crf + 3)
        if point.bitrate < accept_lo:
            return max(crf_min, point.crf - 3)
        # In range: probe a lower CRF to try for higher quality, still in window.
        return max(crf_min, point.crf - 3)

    # 2+ points: bracket / interpolate.
    above = [p for p in history if p.bitrate > accept_hi]
    below = [p for p in history if p.bitrate < accept_lo]
    in_range = [p for p in history if accept_lo <= p.bitrate <= accept_hi]

    if in_range:
        if not above:
            # No overshoot to bracket against — extrapolate toward lower CRF.
            lowest_in_range_crf = min(p.crf for p in in_range)
            if lowest_in_range_crf <= crf_min:
                return None
            crf = _extrapolate_crf(
                history, target_bitrate, crf_min, crf_max,
                direction=-1, crf_nudge_size=2,
            )
            return max(crf_min, min(lowest_in_range_crf - 1, crf))

        nearest_above = min(above, key=lambda p: p.bitrate)  # smallest overshoot
        lowest_in_range = min(in_range, key=lambda p: p.crf)
        if lowest_in_range.crf - nearest_above.crf <= 1:
            return None  # consecutive integers — no untried CRF in between
        crf = interpolate_crf(
            lowest_in_range, nearest_above,
            target_bitrate, crf_min, crf_max,
        )
        return max(nearest_above.crf + 1, min(lowest_in_range.crf - 1, crf))

    if above and below:
        nearest_above = min(above, key=lambda p: p.bitrate)  # smallest overshoot
        nearest_below = max(below, key=lambda p: p.bitrate)  # smallest undershoot
        if nearest_below.crf - nearest_above.crf <= 1:
            return None
        crf = interpolate_crf(
            nearest_below, nearest_above,
            target_bitrate, crf_min, crf_max,
        )
        return max(nearest_above.crf + 1, min(nearest_below.crf - 1, crf))

    if above:
        return _extrapolate_crf(history, target_bitrate, crf_min, crf_max, direction=1)

    if below:
        return _extrapolate_crf(history, target_bitrate, crf_min, crf_max, direction=-1)

    return None


# ── Outer loop ──────────────────────────────────────────────────────────────

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
    crf_ceiling_fallback: int,
    *,
    search_method: SearchMethod = binary_search_next,
    offsets: list[float] | None = None,
    seg_duration: float = 5.0,
    full_encode: bool = False,
    seed_crf: int = -1,
    seed_known: list[CrfPoint] | None = None,
    seed_temp_files: dict[int, Path] | None = None,
    has_audio: bool = True,
) -> CrfResult:
    """Find the optimal CRF using the configured stateless search method.

    The outer loop:

    1. Asks *search_method* for the next CRF to probe.  ``None`` stops
       the search.
    2. If the candidate CRF was already tried, nudges it by ±1 in the
       direction that brings the bitrate closer to the target.  If the
       nudged CRF is also tried (or out of bounds), the search stops
       and returns the most appropriate of the two.
    3. Encodes at the candidate CRF and records the result in *history*.
    4. If the bitrate is in the confident window, returns immediately.
    5. If the CRF is at/above ``crf_max`` and the bitrate still exceeds
       the target, returns ``crf_ceiling_fallback`` with
       ``crf_ceiling_used=True``.
    6. If the CRF is at/below ``crf_min`` and the bitrate is still below
       ``accept_lo``, returns the ``crf_min`` result.

    If ``max_iterations`` is exhausted without convergence, returns the
    best point from history (preferring in-range, then closest from
    below the target, then closest from above).
    """
    accept_lo, accept_hi, confident_lo, confident_hi = _select_windows(windows, full_encode)
    target_bitrate = accept_hi

    history: list[CrfPoint] = list(seed_known or [])
    tried_crfs: set[int] = {p.crf for p in history}
    # Seeded points may carry a reusable encode (e.g. a full-video encode the
    # caller already produced at that CRF), so the search needn't redo it if it
    # ends up selecting that point.  These participate in the same lifecycle as
    # encodes made during the loop, so a non-winning seed file is cleaned up too.
    temp_files_by_crf: dict[int, Path] = dict(seed_temp_files or {})

    log.info(
        "  CRF search: target=%d, accept=[%d, %d], confident=[%d, %d], CRF=[%d, %d]",
        windows.target, accept_lo, accept_hi, confident_lo, confident_hi,
        crf_min, crf_max,
    )
    if seed_known:
        for point in seed_known:
            log.info("  Seeded with known point: CRF=%d -> %d kbps", point.crf, point.bitrate)

    def _cleanup_except(keep_crf: int | None) -> None:
        for c, p in list(temp_files_by_crf.items()):
            if c != keep_crf and p.exists():
                p.unlink(missing_ok=True)
            if c != keep_crf:
                temp_files_by_crf.pop(c, None)

    def _best_from_history() -> CrfPoint | None:
        if not history:
            return None
        in_range = [p for p in history if accept_lo <= p.bitrate <= accept_hi]
        if in_range:
            return min(in_range, key=lambda p: p.crf)  # lowest CRF (highest quality)
        below = [p for p in history if p.bitrate <= accept_hi]
        if below:
            return max(below, key=lambda p: p.bitrate)  # closest to target from below
        return max(history, key=lambda p: p.crf)  # closest to target from above

    def _return_best() -> CrfResult:
        best = _best_from_history()
        if best is None:
            log.warning(
                "  Search did not converge and no history; falling back to CRF=%d",
                crf_max,
            )
            _cleanup_except(None)
            return CrfResult(crf_max, 0, None, crf_ceiling_used=False)
        log.info("  Selected CRF=%d (estimated %d kbps)", best.crf, best.bitrate)
        _cleanup_except(best.crf)
        return CrfResult(best.crf, best.bitrate, temp_files_by_crf.get(best.crf), crf_ceiling_used=False)

    for iteration in range(1, max_iterations + 1):
        candidate = search_method(
            history, crf_min, crf_max, accept_lo, accept_hi, seed_crf,
        )
        if candidate is None:
            log.info("  Search method has no further probes; stopping")
            break

        candidate = max(crf_min, min(crf_max, candidate))

        # Duplicate check: nudge ±1 toward the target.
        if candidate in tried_crfs:
            dup_bitrate = next(
                (p.bitrate for p in history if p.crf == candidate),
                None,
            )
            if dup_bitrate is None:
                # Previous attempt at this CRF failed (no recorded bitrate);
                # default to stepping up to a higher CRF.
                step = +1
            elif dup_bitrate > target_bitrate:
                step = +1  # bitrate too high → higher CRF (lower bitrate)
            else:
                step = -1  # bitrate ≤ target → lower CRF (higher bitrate)
            stepped = candidate + step

            stepped_in_bounds = crf_min <= stepped <= crf_max
            stepped_tried = stepped in tried_crfs

            if not stepped_in_bounds or stepped_tried:
                log.warning(
                    "  Search returned tried CRF=%d and the nudge target "
                    "(CRF=%d) is %s; stopping",
                    candidate, stepped,
                    "out of bounds" if not stepped_in_bounds else "also tried",
                )
                return _return_best()

            log.info(
                "  CRF=%d already tried; nudged to CRF=%d", candidate, stepped,
            )
            candidate = stepped

        tried_crfs.add(candidate)
        bitrate, temp_file = _evaluate_crf_sample(
            input_path, candidate, extra_args, audio_bitrate, preset,
            audio_bitrate_kbps,
            offsets=offsets, seg_duration=seg_duration,
            full_encode=full_encode, has_audio=has_audio,
        )

        if bitrate < 0:
            log.warning(
                "  Iteration %d: CRF=%d -> encode failed", iteration, candidate,
            )
            if temp_file and temp_file.exists():
                temp_file.unlink(missing_ok=True)
            continue

        log.info(
            "  Iteration %d: CRF=%d -> %d kbps", iteration, candidate, bitrate,
        )
        history.append(CrfPoint(candidate, bitrate))
        if temp_file:
            temp_files_by_crf[candidate] = temp_file

        # Confident-zone exit.
        if confident_lo <= bitrate <= confident_hi:
            log.info(
                "  Converged in confident zone [%d, %d]",
                confident_lo, confident_hi,
            )
            _cleanup_except(candidate)
            return CrfResult(
                candidate, bitrate,
                temp_files_by_crf.get(candidate),
                crf_ceiling_used=False,
            )

        # max-crf ceiling: bitrate still too high at the quality floor.
        if candidate >= crf_max and bitrate > accept_hi:
            log.info(
                "  CRF=%d >= max-crf=%d and bitrate (%d kbps) still exceeds "
                "target (%d kbps); using crf-ceiling-fallback=%d",
                candidate, crf_max, bitrate, accept_hi, crf_ceiling_fallback,
            )
            _cleanup_except(None)
            return CrfResult(
                crf_ceiling_fallback, 0, None, crf_ceiling_used=True,
            )

        # crf-min floor: bitrate still too low at the quality ceiling.
        if candidate <= crf_min and bitrate < accept_lo:
            log.info(
                "  CRF=%d <= crf-min=%d and bitrate (%d kbps) still below "
                "accept_lo (%d kbps); stopping at crf-min",
                candidate, crf_min, bitrate, accept_lo,
            )
            _cleanup_except(candidate)
            return CrfResult(
                candidate, bitrate,
                temp_files_by_crf.get(candidate),
                crf_ceiling_used=False,
            )

    return _return_best()
