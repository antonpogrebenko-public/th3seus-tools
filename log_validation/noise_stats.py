"""Pure noise estimators for validating hitl-sensors models against real data.

All functions take 1-D numpy arrays of samples (already restricted to a
stationary segment) and return scalar statistics. No I/O, no ulog knowledge.

Estimator choices:

* White-noise std is estimated from the first difference of the signal:
  for white noise w, Var(w[k] - w[k-1]) = 2*sigma^2, so sigma = std(diff)/sqrt(2).
  This rejects slow bias drift and constant offsets, which a plain std would
  fold into the estimate.

* Noise *density* (IMU) converts a per-sample std to the model's units:
  hitl-sensors adds `noise_density / sqrt(dt) * z`, so the per-sample std is
  `noise_density / sqrt(dt)` and therefore `noise_density = sigma * sqrt(dt)`.

* Allan deviation gives an approximate bias-instability floor and correlation
  time for the Gauss-Markov bias model.
"""

from __future__ import annotations

import math
from dataclasses import dataclass

import numpy as np


def median_dt(timestamps_s: np.ndarray) -> float:
    """Median sample interval (seconds) from a timestamp array."""
    if timestamps_s.size < 2:
        raise ValueError("need at least 2 timestamps")
    diffs = np.diff(np.asarray(timestamps_s, dtype=float))
    diffs = diffs[diffs > 0]
    if diffs.size == 0:
        raise ValueError("timestamps are not strictly increasing")
    return float(np.median(diffs))


def sample_rate_hz(timestamps_s: np.ndarray) -> float:
    """Effective sample rate (Hz) = 1 / median_dt."""
    return 1.0 / median_dt(timestamps_s)


def white_noise_std(samples: np.ndarray) -> float:
    """Drift-robust per-sample white-noise std via first differences.

    sigma = std(diff(x)) / sqrt(2)
    """
    x = np.asarray(samples, dtype=float)
    if x.size < 2:
        raise ValueError("need at least 2 samples")
    diffs = np.diff(x)
    return float(np.std(diffs, ddof=1) / math.sqrt(2.0))


def detrended_std(samples: np.ndarray) -> float:
    """Per-sample std after removing a best-fit linear trend.

    Secondary estimator; more sensitive to non-white low-frequency content
    than `white_noise_std`, useful as a cross-check.
    """
    x = np.asarray(samples, dtype=float)
    if x.size < 3:
        raise ValueError("need at least 3 samples")
    t = np.arange(x.size, dtype=float)
    coeffs = np.polyfit(t, x, 1)
    residual = x - np.polyval(coeffs, t)
    return float(np.std(residual, ddof=1))


def noise_density(samples: np.ndarray, dt_s: float) -> float:
    """IMU noise density (units/sqrt(Hz)) from a stationary segment.

    Uses the drift-robust white-noise std, then scales by sqrt(dt).
    """
    if dt_s <= 0:
        raise ValueError("dt_s must be positive")
    return white_noise_std(samples) * math.sqrt(dt_s)


@dataclass(frozen=True)
class AllanResult:
    """Allan-deviation summary for a bias process."""

    bias_instability: float
    """Minimum of the Allan-deviation curve (model bias_sigma analogue)."""
    correlation_time_s: float
    """Averaging time tau at the Allan-deviation minimum (bias_tau analogue)."""
    taus_s: np.ndarray
    devs: np.ndarray


def allan_deviation(samples: np.ndarray, dt_s: float, n_taus: int = 30) -> AllanResult:
    """Overlapping Allan deviation of a sample series.

    Returns the curve plus the (instability floor, tau-at-floor) summary that
    maps approximately onto the Gauss-Markov bias_sigma / bias_tau parameters.
    The Gauss-Markov Allan signature is not identical to these scalars, so the
    summary is approximate and intended for order-of-magnitude validation.
    """
    x = np.asarray(samples, dtype=float)
    n = x.size
    if n < 16:
        raise ValueError("need at least 16 samples for Allan deviation")

    theta = np.cumsum(x) * dt_s  # integrated signal
    max_m = (n - 1) // 2
    ms = np.unique(np.geomspace(1, max_m, num=n_taus).astype(int))
    ms = ms[ms >= 1]

    taus = []
    devs = []
    for m in ms:
        tau = m * dt_s
        k = n - 2 * m
        if k <= 0:
            continue
        diff = theta[2 * m :] - 2.0 * theta[m : m + k] + theta[:k]
        var = np.sum(diff**2) / (2.0 * tau**2 * k)
        taus.append(tau)
        devs.append(math.sqrt(var))

    taus_arr = np.asarray(taus)
    devs_arr = np.asarray(devs)
    i_min = int(np.argmin(devs_arr))
    return AllanResult(
        bias_instability=float(devs_arr[i_min]),
        correlation_time_s=float(taus_arr[i_min]),
        taus_s=taus_arr,
        devs=devs_arr,
    )


def latlon_to_local_m(
    lat_deg: np.ndarray, lon_deg: np.ndarray, ref_lat_deg: float, ref_lon_deg: float
) -> tuple[np.ndarray, np.ndarray]:
    """Equirectangular lat/lon -> local north/east meters about a reference.

    Matches the small-angle conversion hitl-sensors uses (EARTH_RADIUS_M).
    """
    earth_radius_m = 6371000.0
    ref_lat_rad = math.radians(ref_lat_deg)
    north = np.radians(np.asarray(lat_deg, float) - ref_lat_deg) * earth_radius_m
    east = (
        np.radians(np.asarray(lon_deg, float) - ref_lon_deg)
        * earth_radius_m
        * math.cos(ref_lat_rad)
    )
    return north, east


def horizontal_position_noise(north_m: np.ndarray, east_m: np.ndarray) -> float:
    """Stationary horizontal GPS noise std (meters), drift-robust per axis.

    Combines the two axes as the RMS of their independent white-noise stds.
    """
    sn = white_noise_std(north_m)
    se = white_noise_std(east_m)
    return float(math.sqrt(0.5 * (sn**2 + se**2)))
