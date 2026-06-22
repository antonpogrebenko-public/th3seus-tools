"""Detect the on-ground / stationary window of a flight from gyro motion.

Pre-arm rest is the cleanest noise floor: motors off, no airframe vibration,
true angular rate is zero. We find the longest contiguous run where the gyro
magnitude stays below a small threshold. This avoids depending on PX4 arming
enum values (which vary across firmware versions).

The threshold is applied to a short *moving average of each (signed) axis*, not
the raw per-sample magnitude. Raw high-rate gyro noise can itself exceed the
threshold on every sample; smoothing each signed axis lets zero-mean noise
average toward zero while genuine low-frequency rotation survives.
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np

# rad/s; smoothed rest rate is ~0, real motion is well above this.
DEFAULT_GYRO_THRESHOLD = 0.03
# Seconds of moving-average smoothing applied to each gyro axis.
DEFAULT_SMOOTH_S = 0.2
# Minimum stationary duration to trust statistics on.
DEFAULT_MIN_DURATION_S = 3.0


def _smoothed_magnitude(t_s: np.ndarray, gyro_xyz: np.ndarray, smooth_s: float) -> np.ndarray:
    """Magnitude of the per-axis moving average (noise averaged toward zero)."""
    g = np.asarray(gyro_xyz, float)
    diffs = np.diff(np.asarray(t_s, float))
    diffs = diffs[diffs > 0]
    dt = float(np.median(diffs)) if diffs.size else 0.0
    win = max(1, int(round(smooth_s / dt))) if dt > 0 else 1
    if win <= 1:
        return np.linalg.norm(g, axis=1)
    kernel = np.ones(win) / win
    smoothed = np.column_stack(
        [np.convolve(g[:, a], kernel, mode="same") for a in range(g.shape[1])]
    )
    return np.linalg.norm(smoothed, axis=1)


@dataclass(frozen=True)
class Window:
    """A stationary time window as inclusive index bounds and times."""

    i0: int
    i1: int
    t0_s: float
    t1_s: float

    @property
    def duration_s(self) -> float:
        return self.t1_s - self.t0_s


def find_stationary_window(
    t_s: np.ndarray,
    gyro_xyz: np.ndarray,
    gyro_threshold: float = DEFAULT_GYRO_THRESHOLD,
    min_duration_s: float = DEFAULT_MIN_DURATION_S,
    smooth_s: float = DEFAULT_SMOOTH_S,
) -> Window | None:
    """Longest contiguous run where the smoothed gyro magnitude < threshold.

    Returns None if no qualifying run reaches `min_duration_s`.
    """
    mag = _smoothed_magnitude(t_s, gyro_xyz, smooth_s)
    quiet = mag < gyro_threshold

    best: tuple[int, int] | None = None
    best_len = 0
    start = None
    for i, q in enumerate(quiet):
        if q and start is None:
            start = i
        elif not q and start is not None:
            if i - start > best_len:
                best_len = i - start
                best = (start, i - 1)
            start = None
    if start is not None and len(quiet) - start > best_len:
        best = (start, len(quiet) - 1)

    if best is None:
        return None
    i0, i1 = best
    t0, t1 = float(t_s[i0]), float(t_s[i1])
    if t1 - t0 < min_duration_s:
        return None
    return Window(i0=i0, i1=i1, t0_s=t0, t1_s=t1)


def slice_to_window(t_s: np.ndarray, window: Window) -> np.ndarray:
    """Boolean mask selecting samples of another topic within the time window.

    Used to apply a window found on the gyro to baro/mag/gps series that have
    different timestamps and rates.
    """
    t = np.asarray(t_s, float)
    return (t >= window.t0_s) & (t <= window.t1_s)
