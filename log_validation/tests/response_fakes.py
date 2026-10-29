"""Synthetic quadrotor logs for the response pass tests (FakeULog shaped)."""

from __future__ import annotations

from dataclasses import dataclass, field

import numpy as np

from log_validation.tests.fakes import FakeULog

SPI_ICM20689_GYRO_ID = 0x3C000A
AIRFRAME = 4001
RATE_HZ = 800.0
GAIN = 40.0
DELAY_S = 0.010
TAU_S = 0.020
GYRO_CUTOFF_HZ = 40.0
DGYRO_CUTOFF_HZ = 30.0
AXES = ("roll", "pitch", "yaw")


def reference_lowpass(u: np.ndarray, dt_s: float, tau_s: float) -> np.ndarray:
    """Plain first-order recursion y[n] = y[n-1] + a (u[n] - y[n-1]), y[-1] = u[0]."""
    if tau_s <= 0:
        return np.array(u, dtype=float)
    a = dt_s / (tau_s + dt_s)
    out = np.empty(len(u))
    state = float(u[0])
    for i, value in enumerate(u):
        state += a * (value - state)
        out[i] = state
    return out


def simulate_plant(
    u: np.ndarray, dt_s: float, gain: float, delay_s: float, tau_s: float,
    noise_std: float, rng: np.random.Generator,
) -> np.ndarray:
    """gain * lowpass(u) delayed by a whole number of samples, plus white noise."""
    shift = int(round(delay_s / dt_s))
    lagged = reference_lowpass(u, dt_s, tau_s)
    delayed = np.concatenate([np.full(shift, lagged[0]), lagged[: len(lagged) - shift]])
    return gain * delayed + rng.normal(0.0, noise_std, len(u))


def sum_of_sines(t_s: np.ndarray, amplitude: float, rng: np.random.Generator) -> np.ndarray:
    """A varied setpoint: sines from 0.5 to 15 Hz with random phases."""
    freqs = np.array([0.5, 1.1, 2.3, 3.7, 5.3, 7.1, 9.7, 12.1, 15.0])
    phases = rng.uniform(0, 2 * np.pi, len(freqs))
    waves = np.sin(2 * np.pi * freqs[:, None] * t_s[None, :] + phases[:, None])
    return amplitude * waves.sum(axis=0) / np.sqrt(len(freqs) / 2)


@dataclass(frozen=True)
class LoopSpec:
    """A rate loop around the plant: P on the rate error, D on the measured derivative."""

    rate_hz: float = RATE_HZ
    seconds: float = 60.0
    p_gain: float = 0.15
    d_gain: float = 0.004
    ref_amplitude: float = 1.0  # rad/s, sum of sines into the rate setpoint
    process_noise_std: float = 30.0  # rad/s^2 on the angular acceleration, before colouring
    process_noise_tau_s: float = 0.03  # first-order colouring of the process noise
    gyro_noise_std: float = 0.002  # rad/s on the measured rate
    dgyro_tau_s: float = 0.002  # first-order filter on the measured derivative
    seed: int = 21


@dataclass(frozen=True)
class LoopRun:
    """Logged signals of a closed-loop run, one value per control step."""

    dt_s: float
    rate_sp: np.ndarray  # the reference, rad/s
    torque: np.ndarray  # the controller output u
    accel: np.ndarray  # the measured, filtered derivative y, which the D term also uses
    rate: np.ndarray  # the measured rate


def simulate_rate_loop(spec: LoopSpec = LoopSpec()) -> LoopRun:
    """The plant of simulate_plant (GAIN, DELAY_S, TAU_S) under a P + D rate loop.

    From u to the logged y the lag is the plant's DELAY_S + TAU_S plus the measurement
    path: the dgyro filter (dgyro_tau_s) and half a sample of the backward difference.
    """
    rng = np.random.default_rng(spec.seed)
    dt = 1.0 / spec.rate_hz
    n = int(spec.seconds * spec.rate_hz)
    ref = sum_of_sines(np.arange(n) * dt, spec.ref_amplitude, rng)
    process = reference_lowpass(rng.normal(0.0, spec.process_noise_std, n), dt,
                                spec.process_noise_tau_s)
    gyro_noise = rng.normal(0.0, spec.gyro_noise_std, n)
    shift = int(round(DELAY_S / dt))
    a_plant, a_dgyro = dt / (TAU_S + dt), dt / (spec.dgyro_tau_s + dt)
    lagged = np.zeros(n)
    u, y, measured = np.zeros(n), np.zeros(n), np.zeros(n)
    omega = state = y_state = previous = 0.0
    for k in range(n):
        measured[k] = omega + gyro_noise[k]
        y_state += a_dgyro * ((measured[k] - previous) / dt - y_state)
        previous, y[k] = measured[k], y_state
        u[k] = spec.p_gain * (ref[k] - measured[k]) - spec.d_gain * y[k]
        state += a_plant * (u[k] - state)
        lagged[k] = state
        omega += (GAIN * (lagged[k - shift] if k >= shift else 0.0) + process[k]) * dt
    return LoopRun(dt, ref, u, y, measured)


@dataclass(frozen=True)
class QuadSpec:
    """What a synthetic quadrotor log holds; defaults give a clean full-rate flight."""

    duration_s: float = 40.0
    takeoff_s: float = 5.0
    landing_s: float = 35.0
    rate_hz: float = RATE_HZ
    seed: int = 3
    constant_axes: tuple[int, ...] = ()  # axes with a constant torque of 0.01
    with_derivative: bool = True
    with_torque: bool = True
    noise_only: bool = False  # angular acceleration unrelated to the torque
    params: dict = field(default_factory=dict)
    board: str = "PX4_FMU_V5"
    gain: float = GAIN  # the plant: angular acceleration per unit torque
    delay_s: float = DELAY_S
    tau_s: float = TAU_S
    # The rate setpoint, the fit's instrument: "torque" follows the torque setpoint,
    # "unrelated" is an independent signal, "none" leaves the topic out.
    instrument: str = "torque"
    rates_sp_hz: float | None = None  # its logged rate; None: rate_hz


def _rates_setpoint(t_s: np.ndarray, torque: np.ndarray, spec: QuadSpec,
                    rng: np.random.Generator) -> dict[str, np.ndarray]:
    t_sp = np.arange(0.0, spec.duration_s, 1.0 / (spec.rates_sp_hz or spec.rate_hz))
    out = {"timestamp": t_sp * 1e6}
    for k, name in enumerate(("roll", "pitch", "yaw")):
        if spec.instrument == "unrelated":
            # Broadband noise: lags of a sum of sines at the torque's own frequencies
            # would span the torque and make a strong instrument.
            source = rng.normal(0.0, 1.0, len(t_s))
        else:
            source = 10.0 * torque[:, k] + rng.normal(0.0, 0.01, len(t_s))
        out[name] = np.interp(t_sp, t_s, source)
    return out


def _segment_topics(n_lp: int, t_lp_us: np.ndarray, spec: QuadSpec) -> dict:
    t_s = t_lp_us * 1e-6
    in_air = (t_s >= spec.takeoff_s) & (t_s <= spec.landing_s)
    zeros = np.zeros(n_lp)
    return {
        "vehicle_status": {"timestamp": t_lp_us, "arming_state": np.full(n_lp, 2.0)},
        "vehicle_land_detected": {"timestamp": t_lp_us, "landed": np.where(in_air, 0.0, 1.0)},
        "vehicle_local_position": {
            "timestamp": t_lp_us, "z": np.where(in_air, -5.0, 0.0),
            "vx": zeros, "vy": zeros, "vz": zeros,
        },
    }


def _torque(t_s: np.ndarray, spec: QuadSpec, rng: np.random.Generator) -> np.ndarray:
    cols = []
    for axis in range(3):
        if axis in spec.constant_axes:
            cols.append(0.01 + rng.normal(0.0, 0.001, len(t_s)))
        else:
            cols.append(sum_of_sines(t_s, 0.1, rng))
    return np.column_stack(cols)


def quad_datasets(spec: QuadSpec) -> dict[str, dict[str, np.ndarray]]:
    """The topics of one synthetic flight: setpoint, rate and its derivative, segments."""
    rng = np.random.default_rng(spec.seed)
    dt = 1.0 / spec.rate_hz
    n = int(spec.duration_s * spec.rate_hz)
    t_s = np.arange(n) * dt
    t_us = t_s * 1e6
    torque = _torque(t_s, spec, rng)
    accel = np.column_stack([
        rng.normal(0.0, 4.0, n) if spec.noise_only
        else simulate_plant(torque[:, k], dt, spec.gain, spec.delay_s, spec.tau_s, 0.5, rng)
        for k in range(3)
    ])
    rate = np.cumsum(accel, axis=0) * dt
    datasets: dict[str, dict[str, np.ndarray]] = {}
    if spec.with_torque:
        datasets["vehicle_torque_setpoint"] = {
            "timestamp": t_us, **{f"xyz[{k}]": torque[:, k] for k in range(3)}}
    rate_topic = {"timestamp": t_us + 300.0, "timestamp_sample": t_us,
                  **{f"xyz[{k}]": rate[:, k] for k in range(3)}}
    if spec.with_derivative:
        rate_topic.update({f"xyz_derivative[{k}]": accel[:, k] for k in range(3)})
    datasets["vehicle_angular_velocity"] = rate_topic
    if spec.instrument != "none":
        datasets["vehicle_rates_setpoint"] = _rates_setpoint(t_s, torque, spec, rng)
    n_lp = int(spec.duration_s * 50)
    datasets.update(_segment_topics(n_lp, np.arange(n_lp) * 2e4, spec))
    return datasets


def quad_params(**overrides: float) -> dict[str, float]:
    params = {
        "MAV_TYPE": 2, "SYS_AUTOSTART": AIRFRAME, "CAL_GYRO0_ID": SPI_ICM20689_GYRO_ID,
        "IMU_GYRO_CUTOFF": GYRO_CUTOFF_HZ, "IMU_DGYRO_CUTOFF": DGYRO_CUTOFF_HZ,
    }
    params.update(overrides)
    return {k: v for k, v in params.items() if v is not None}


def quad_log(spec: QuadSpec = QuadSpec(), datasets: dict | None = None) -> FakeULog:
    return FakeULog(
        datasets if datasets is not None else quad_datasets(spec),
        params=quad_params(**spec.params),
        info={"ver_hw": spec.board},
    )
