"""Tests for noise estimators using synthetic signals with known parameters.

No real logs required: we generate signals matching the hitl-sensors models
and assert the estimators recover the injected parameters.
"""

from __future__ import annotations

import math

import numpy as np
import pytest

from log_validation import noise_stats as ns


def test_white_noise_std_recovers_sigma():
    rng = np.random.default_rng(0)
    sigma = 0.0064
    x = rng.normal(0.0, sigma, size=200_000)
    est = ns.white_noise_std(x)
    assert abs(est - sigma) / sigma < 0.02


def test_white_noise_std_rejects_linear_drift():
    rng = np.random.default_rng(1)
    sigma = 0.01
    n = 200_000
    drift = np.linspace(0.0, 5.0, n)  # large slow drift
    x = drift + rng.normal(0.0, sigma, size=n)
    est = ns.white_noise_std(x)
    # First-difference estimator should ignore the smooth ramp.
    assert abs(est - sigma) / sigma < 0.05


def test_noise_density_recovers_model_density():
    # Mirror hitl-sensors IMU: per-sample sigma = density / sqrt(dt).
    rng = np.random.default_rng(2)
    density = 0.00637  # m/s^2/sqrt(Hz), accel default
    dt = 1.0 / 1000.0  # 1 kHz
    per_sample_sigma = density / math.sqrt(dt)
    x = 9.81 + rng.normal(0.0, per_sample_sigma, size=300_000)
    est = ns.noise_density(x, dt)
    assert abs(est - density) / density < 0.03


def test_sample_rate_from_timestamps():
    t = np.arange(0, 10, 0.004)  # 250 Hz
    assert abs(ns.sample_rate_hz(t) - 250.0) < 1.0


def test_median_dt_rejects_nonincreasing():
    with pytest.raises(ValueError):
        ns.median_dt(np.array([1.0, 1.0, 1.0]))


def test_horizontal_position_noise_combines_axes():
    rng = np.random.default_rng(3)
    sigma = 1.5  # GPS horizontal default
    north = rng.normal(0.0, sigma, size=50_000)
    east = rng.normal(0.0, sigma, size=50_000)
    est = ns.horizontal_position_noise(north, east)
    assert abs(est - sigma) / sigma < 0.05


def test_latlon_to_local_zero_at_reference():
    lat = np.array([40.0])
    lon = np.array([-105.0])
    north, east = ns.latlon_to_local_m(lat, lon, 40.0, -105.0)
    assert abs(north[0]) < 1e-6
    assert abs(east[0]) < 1e-6


def test_latlon_to_local_north_positive():
    # 0.001 deg latitude north ~= 111 m
    north, _ = ns.latlon_to_local_m(np.array([40.001]), np.array([-105.0]), 40.0, -105.0)
    assert 100.0 < north[0] < 120.0


def test_allan_deviation_white_noise_slope():
    # For white noise, Allan deviation falls as tau^-0.5.
    rng = np.random.default_rng(4)
    dt = 0.01
    x = rng.normal(0.0, 1.0, size=100_000)
    res = ns.allan_deviation(x, dt)
    # Check the curve roughly halves per decade of tau (slope ~ -0.5).
    log_tau = np.log10(res.taus_s)
    log_dev = np.log10(res.devs)
    slope = np.polyfit(log_tau, log_dev, 1)[0]
    assert -0.6 < slope < -0.4
