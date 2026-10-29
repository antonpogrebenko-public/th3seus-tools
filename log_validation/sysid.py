"""System identification helpers for the response pass.

- ``uniform_resample`` / ``resample_on``: put a logged series on a uniform time grid,
  dropping repeated and backward timestamps first.
- ``first_order_lowpass``: the causal first-order lag, exact and vectorized.
- ``fit_first_order_plant``: regress a response on its input passed through a pure
  delay and a first-order lag, by least squares at every point of a delay and time
  constant grid, keeping the point with the highest R^2.
- ``butterworth2_group_delay_s`` / ``first_order_group_delay_s``: the low-frequency
  group delay of PX4's second-order low-pass (``LowPassFilter2p``) and of its
  first-order ``AlphaFilter``; ``response.py`` says which of them sit on the logged
  angular acceleration path and cites the PX4 source.

numpy only. No function changes its inputs.
"""

from __future__ import annotations

import math
from collections.abc import Sequence
from dataclasses import dataclass

import numpy as np

# first_order_lowpass works in blocks inside which the weight r**-k grows to at most
# e**_MAX_BLOCK_GROWTH: far below the float64 limit (about e**709), and the result
# keeps full precision because each block's sum is scaled back by r**k at once.
_MAX_BLOCK_GROWTH = 300.0
# The fewest response samples, over all runs, that a plant fit accepts.
MIN_FIT_SAMPLES = 100
# Tolerance when a delay is converted to a whole number of samples plus a fraction.
_SAMPLE_EPS = 1e-9


@dataclass(frozen=True)
class PlantFit:
    """y = gain * lowpass(u, tau_s) delayed by delay_s, plus an offset; r2 in [0, 1]."""

    gain: float
    delay_s: float
    tau_s: float
    r2: float

    @property
    def lag_s(self) -> float:
        """Effective lag: the pure delay plus the first-order time constant."""
        return self.delay_s + self.tau_s


# ---------------------------------------------------------------------------
# Resampling
# ---------------------------------------------------------------------------


def increasing_samples(t_s: np.ndarray, x: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    """New arrays without the samples whose stamp is not finite or not later than all before it.

    ``x`` may be one value per stamp or one row per stamp.
    """
    t = np.asarray(t_s, dtype=float)
    values = np.asarray(x, dtype=float)
    finite = np.isfinite(t)
    t, values = t[finite], values[finite]
    if t.size == 0:
        return t, values
    keep = np.ones(t.size, dtype=bool)
    keep[1:] = t[1:] > np.maximum.accumulate(t)[:-1]
    return t[keep], values[keep]


def resample_on(t_s: np.ndarray, x: np.ndarray, grid_s: np.ndarray) -> np.ndarray:
    """x linearly interpolated at grid_s from its increasing samples; NaN outside their span."""
    t, values = increasing_samples(t_s, x)
    grid = np.asarray(grid_s, dtype=float)
    if t.size == 0:
        return np.full(grid.size, np.nan)
    return np.interp(grid, t, values, left=np.nan, right=np.nan)


def uniform_resample(
    t_s: np.ndarray, x: np.ndarray, dt_s: float
) -> tuple[np.ndarray, np.ndarray]:
    """(grid, x on grid): drop non-increasing stamps, then interpolate linearly every dt_s.

    The grid starts at the first kept stamp and ends at or before the last one.
    """
    if not dt_s > 0:
        raise ValueError(f"dt_s must be positive, got {dt_s}")
    t, values = increasing_samples(t_s, x)
    if t.size == 0:
        return np.empty(0), np.empty(0)
    n = int(math.floor((t[-1] - t[0]) / dt_s + _SAMPLE_EPS)) + 1
    grid = t[0] + np.arange(n) * dt_s
    return grid, np.interp(grid, t, values)


# ---------------------------------------------------------------------------
# First-order low-pass
# ---------------------------------------------------------------------------


def _block_entry_states(block_ends: np.ndarray, decay: float, initial: float) -> np.ndarray:
    """State entering each block: s[0] = initial, s[b + 1] = decay * s[b] + block_ends[b]."""
    states = np.empty(block_ends.size)
    state = initial
    for b, end in enumerate(block_ends):
        states[b] = state
        state = decay * state + end
    return states


def first_order_lowpass(u: np.ndarray, dt_s: float, tau_s: float) -> np.ndarray:
    """Causal first-order low-pass: y[n] = y[n-1] + a (u[n] - y[n-1]), a = dt / (tau + dt).

    The filter starts settled at u[0]; tau_s <= 0 returns a copy of u. Its
    low-frequency delay is tau_s exactly. Computed without a per-sample loop:
    inside a block, y[k] = a r**k cumsum(r**-j u[j]) with r = 1 - a, and the
    state carried into each block decays as r**(k + 1).
    """
    out = np.array(u, dtype=float)
    if tau_s <= 0 or out.size == 0:
        return out
    a = dt_s / (tau_s + dt_s)
    log_r = math.log1p(-a)
    block = max(1, min(out.size, int(_MAX_BLOCK_GROWTH / -log_r)))
    n_blocks = -(-out.size // block)
    padded = np.zeros(n_blocks * block)
    padded[: out.size] = out
    blocks = padded.reshape(n_blocks, block)
    k = np.arange(block)
    decay = np.exp(log_r * k)
    local = a * decay * np.cumsum(blocks * np.exp(-log_r * k), axis=1)
    entry = _block_entry_states(local[:, -1], math.exp(log_r * block), float(out[0]))
    filtered = local + entry[:, None] * (decay * math.exp(log_r))
    return filtered.reshape(-1)[: out.size]


# ---------------------------------------------------------------------------
# Plant fit
# ---------------------------------------------------------------------------


def _lag_moments(
    lp: np.ndarray, target: np.ndarray, warmup: int
) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    """Moments of A_n = lp[warmup - n : len - n] for n = 0..warmup.

    Returns (A_n . target, sum A_n, A_n . A_n, A_n . A_(n+1)); the last is
    meaningful for n < warmup only.
    """
    n = np.arange(warmup + 1)
    lo, hi = warmup - n, lp.size - n
    cross = np.array([np.dot(lp[a:b], target) for a, b in zip(lo, hi)])
    zero = np.zeros(1)
    sums = np.concatenate((zero, np.cumsum(lp)))
    squares = np.concatenate((zero, np.cumsum(lp * lp)))
    neighbours = np.concatenate((zero, zero, np.cumsum(lp[1:] * lp[:-1])))
    return cross, sums[hi] - sums[lo], squares[hi] - squares[lo], neighbours[hi] - neighbours[lo]


def _delay_sums(
    moments: tuple[np.ndarray, ...], whole: np.ndarray, frac: np.ndarray
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Sum x, sum x*y and sum x*x of x = lp delayed by whole + frac samples.

    Between samples x is interpolated linearly: x = (1 - frac) A_whole + frac A_(whole+1).
    """
    cross, sums, squares, neighbours = moments
    w0, w1 = 1.0 - frac, frac
    sx = w0 * sums[whole] + w1 * sums[whole + 1]
    sxy = w0 * cross[whole] + w1 * cross[whole + 1]
    sxx = (w0 * w0 * squares[whole] + 2.0 * w0 * w1 * neighbours[whole]
           + w1 * w1 * squares[whole + 1])
    return sx, sxy, sxx


def _usable_runs(
    runs: Sequence[tuple[np.ndarray, np.ndarray]], warmup: int
) -> list[tuple[np.ndarray, np.ndarray]]:
    usable = []
    for u, y in runs:
        u_arr, y_arr = np.asarray(u, dtype=float), np.asarray(y, dtype=float)
        if u_arr.shape != y_arr.shape or u_arr.ndim != 1:
            raise ValueError("each run needs 1-D input and response of the same length")
        if not (np.all(np.isfinite(u_arr)) and np.all(np.isfinite(y_arr))):
            raise ValueError("runs must hold finite samples only")
        if u_arr.size > warmup:
            usable.append((u_arr, y_arr))
    if sum(u.size - warmup for u, _ in usable) < MIN_FIT_SAMPLES:
        raise ValueError(f"too few samples to fit (need {MIN_FIT_SAMPLES} after the longest delay)")
    return usable


def _r2_and_gain(
    sums: np.ndarray, count: int, sy: float, syy: float
) -> tuple[np.ndarray, np.ndarray]:
    """R^2 and least-squares gain (with an offset) from the stacked (sx, sxy, sxx) sums."""
    sx, sxy, sxx = sums
    cov = sxy - sx * sy / count
    var_x = sxx - sx * sx / count
    var_y = syy - sy * sy / count
    with np.errstate(divide="ignore", invalid="ignore"):
        gain = cov / var_x
        r2 = cov * cov / (var_x * var_y)
    valid = (var_x > 0) & (var_y > 0) & np.isfinite(r2)
    return np.where(valid, np.clip(r2, 0.0, 1.0), -np.inf), gain


def fit_first_order_plant_runs(
    runs: Sequence[tuple[np.ndarray, np.ndarray]],
    dt_s: float,
    delays_s: Sequence[float],
    taus_s: Sequence[float],
) -> PlantFit:
    """Fit one plant to several runs of (u, y) on the same dt_s grid.

    Each run is filtered and delayed on its own, so no delay reaches across a
    gap between runs; the least-squares sums of all runs are then added. Every
    grid point is scored on the same response samples: those of each run after
    its first longest-delay samples.
    """
    delays, taus = np.asarray(delays_s, dtype=float), np.asarray(taus_s, dtype=float)
    if not dt_s > 0 or delays.size == 0 or taus.size == 0 or np.any(delays < 0):
        raise ValueError("need dt_s > 0 and non-empty grids of non-negative delays")
    shifts = delays / dt_s
    whole = np.floor(shifts + _SAMPLE_EPS).astype(int)
    frac = np.clip(shifts - whole, 0.0, 1.0)
    warmup = int(whole.max()) + 1
    usable = _usable_runs(runs, warmup)
    all_u = np.concatenate([u for u, _ in usable])
    targets = [y[warmup:] for _, y in usable]
    all_y = np.concatenate(targets)
    if np.ptp(all_u) == 0 or np.ptp(all_y) == 0:
        raise ValueError("the input or the response has no variation over the fitted samples")
    u_mean, y_mean = float(np.mean(all_u)), float(np.mean(all_y))
    count = sum(t.size for t in targets)
    sy = sum(float(np.sum(t - y_mean)) for t in targets)
    syy = sum(float(np.dot(t - y_mean, t - y_mean)) for t in targets)

    r2 = np.empty((taus.size, delays.size))
    gain = np.empty_like(r2)
    for i, tau in enumerate(taus):
        sums = np.zeros((3, delays.size))
        for (u, _), target in zip(usable, targets):
            lowpassed = first_order_lowpass(u - u_mean, dt_s, tau)
            moments = _lag_moments(lowpassed, target - y_mean, warmup)
            sums += np.array(_delay_sums(moments, whole, frac))
        r2[i], gain[i] = _r2_and_gain(sums, count, sy, syy)
    if not np.any(np.isfinite(r2)):
        raise ValueError("the input has no variation over the fitted samples")
    i, j = np.unravel_index(int(np.argmax(r2)), r2.shape)
    return PlantFit(gain=float(gain[i, j]), delay_s=float(delays[j]), tau_s=float(taus[i]),
                    r2=float(r2[i, j]))


def fit_first_order_plant(
    u: np.ndarray,
    y: np.ndarray,
    dt_s: float,
    delays_s: Sequence[float],
    taus_s: Sequence[float],
) -> PlantFit:
    """Regress y on u delayed and low-passed, for every grid point; keep the highest R^2.

    u and y are uniformly sampled every dt_s. The model is y = gain * x + offset
    with x = first_order_lowpass(u, tau) delayed by delay (linear interpolation
    between samples), solved by least squares at each (delay, tau).
    """
    return fit_first_order_plant_runs([(u, y)], dt_s, delays_s, taus_s)


# ---------------------------------------------------------------------------
# Filter group delays
# ---------------------------------------------------------------------------


def butterworth2_group_delay_s(cutoff_hz: float) -> float:
    """Low-frequency group delay of a second-order Butterworth low-pass: sqrt(2) / (2 pi fc).

    PX4's LowPassFilter2p; 0 for a cutoff <= 0, which disables it.
    """
    if not cutoff_hz > 0:
        return 0.0
    return math.sqrt(2.0) / (2.0 * math.pi * cutoff_hz)


def first_order_group_delay_s(cutoff_hz: float) -> float:
    """Low-frequency group delay of a first-order low-pass: its time constant 1 / (2 pi fc).

    PX4's AlphaFilter set by cutoff frequency; 0 for a cutoff <= 0, which disables it.
    """
    if not cutoff_hz > 0:
        return 0.0
    return 1.0 / (2.0 * math.pi * cutoff_hz)
