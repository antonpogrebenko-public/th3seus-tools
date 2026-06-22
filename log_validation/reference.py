"""hitl-sensors model defaults, mirrored for comparison.

Single source of truth is the Rust crate `hitl-sensors`. These values are
copied from the `Default` impls and must be kept in sync if the Rust defaults
change:

* IMU   -> hitl-sensors/src/imu.rs   (ImuConfig::default)
* GPS   -> hitl-sensors/src/gps.rs   (GpsConfig::default)
* Baro  -> hitl-sensors/src/baro.rs  (BaroConfig::default)
* Mag   -> hitl-sensors/src/mag.rs   (MagConfig::default)

The crate also documents "HITL recommended" overrides in hitl-sensors/CLAUDE.md
(e.g. gyro/accel bias disabled, tighter GPS noise). Those are deployment tuning,
not the physical model, so validation compares against the code defaults below.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ModelParam:
    """A single model parameter with its expected value and unit."""

    name: str
    value: float
    unit: str
    note: str = ""


# IMU (ImuConfig::default)
IMU = {
    "accel_noise_density": ModelParam("accel_noise_density", 0.032669, "m/s^2/sqrt(Hz)"),
    "gyro_noise_density": ModelParam("gyro_noise_density", 0.002937, "rad/s/sqrt(Hz)"),
    "accel_bias_sigma": ModelParam("accel_bias_sigma", 0.003, "m/s^2", "Gauss-Markov steady-state"),
    "accel_bias_tau": ModelParam("accel_bias_tau", 300.0, "s"),
    "gyro_bias_sigma": ModelParam("gyro_bias_sigma", 1e-4, "rad/s", "Gauss-Markov steady-state"),
    "gyro_bias_tau": ModelParam("gyro_bias_tau", 100.0, "s"),
}

# GPS (GpsConfig::default)
GPS = {
    "horizontal_noise_sigma": ModelParam("horizontal_noise_sigma", 1.5, "m"),
    "altitude_noise_sigma": ModelParam("altitude_noise_sigma", 3.0, "m"),
    "velocity_noise_sigma": ModelParam("velocity_noise_sigma", 0.1, "m/s"),
    "update_rate_hz": ModelParam("update_rate_hz", 5.0, "Hz"),
    "delay_ms": ModelParam("delay_ms", 120.0, "ms", "not auto-measured yet"),
}

# Barometer (BaroConfig::default)
BARO = {
    "noise_sigma": ModelParam("noise_sigma", 0.15, "m"),
}

# Magnetometer (MagConfig::default)
MAG = {
    "noise_sigma_gauss": ModelParam("noise_sigma_gauss", 0.005, "Gauss"),
}
