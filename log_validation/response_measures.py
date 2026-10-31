"""Per-log measurements of the response pass besides the effectiveness fit.

Each group returns ``(measurements, errors)`` and never raises for a data problem: a
failing group adds an error naming the cause and leaves the other groups alone. All
values are finite (``_finite_only`` drops and reports any that are not).

- Hover (``hover_measurements``): ``hover.thrust`` is the median of
  ``-vehicle_thrust_setpoint.xyz[2]`` over the HOVER segments (each trimmed by
  TRIM_S at both ends, as the in-air windows are; a segment not longer than
  2 * TRIM_S is left out);
  ``hover.command`` is the motor command that gives it under PX4's thrust model with
  the log's ``THR_MDL_FAC`` (``sysid.hover_command``); ``hover.thr_mdl_fac`` and
  ``hover.mpc_thr_hover`` are the parameters; ``hover.estimate`` is the median
  ``hover_thrust_estimate.hover_thrust`` over the HOVER segments, valid samples only
  when the topic has a ``valid`` field. A missing or out-of-range THR_MDL_FAC gives no
  command and an error; the thrust is still reported.
- In-flight gyro noise (``inflight_noise_measurements``):
  ``inflight_gyro_noise.<axis>`` is ``sysid.inflight_noise_std`` of
  ``vehicle_angular_velocity.xyz[k]`` in the HOVER segments, with the moving-average
  window that ``noise_cutoff_hz`` sets (see that function), and
  ``inflight_gyro_rate_hz`` the logged rate of that topic there. A log without a hover
  segment gets one error for the whole hover group (``hover_group``).
- Motor lag (``motor_lag_measurements``): per ESC, ``esc_status.esc[i].esc_rpm`` (minus
  its mean over the used windows) against ``actuator_motors.control[i]``, fitted as a
  delay plus a first-order lag; ``motor_lag_s`` and ``motor_lag_r2`` are the medians
  over the ESCs that fitted.
- Rates (``rate_measurements``): ``rate.torque_setpoint_hz`` and
  ``rate.angular_velocity_hz`` from the topics' own timestamps.
- ``param_measurements`` (IMU_GYRO_RATEMAX, IMU_GYRO_CUTOFF, IMU_DGYRO_CUTOFF and
  THR_MDL_FAC, only when present, with or without a hover segment) and
  ``cell_count_measurements`` (first positive battery cell count).

None of these functions changes the log or any array it reads.
"""

from __future__ import annotations

import math
from collections.abc import Callable
from typing import Any

import numpy as np

from .flight_segments import FlightSegment, SegmentKind
from .noise_stats import sample_rate_hz
from .sysid import (
    PlantFit,
    fit_first_order_plant_runs,
    hover_command,
    increasing_samples,
    inflight_noise_std,
    resample_on,
)
from .ulog_loader import US_TO_S

Window = tuple[float, float]
Measured = tuple[dict[str, float], list[str]]

AXES = ("roll", "pitch", "yaw")
TOPIC_RATE = "vehicle_angular_velocity"
TOPIC_TORQUE = "vehicle_torque_setpoint"
TOPIC_THRUST = "vehicle_thrust_setpoint"
TOPIC_HOVER_ESTIMATE = "hover_thrust_estimate"
TOPIC_ESC = "esc_status"
TOPIC_MOTORS = "actuator_motors"
TOPIC_BATTERY = "battery_status"
GYRO_PARAMS = ("IMU_GYRO_RATEMAX", "IMU_GYRO_CUTOFF", "IMU_DGYRO_CUTOFF")
# Recorded as param.<name>. THR_MDL_FAC is here as well as in hover.thr_mdl_fac so a
# log without a hover segment still carries the thrust model its effectiveness needs.
RECORDED_PARAMS = (*GYRO_PARAMS, "THR_MDL_FAC")

# Motor lag fit grid: large props are slower than the body-rate plant, so delays run
# 0 to 40 ms and time constants 0 to 150 ms, in 1 ms steps.
MOTOR_DELAYS_S = tuple(i * 1e-3 for i in range(41))
MOTOR_TAUS_S = tuple(i * 1e-3 for i in range(151))
# ESC RPM and the motor command must each be logged at least this often. A lag of
# 30 ms is about 3 samples at 100 Hz (10 ms apart); slower logging leaves under 3
# samples per time constant and the fit's 1 ms grid would only interpolate between
# far-apart samples, so the lag is not resolved. A median 5 % below is accepted
# (scheduling jitter), as for the rate setpoint in response.py.
MIN_ESC_RPM_RATE_HZ = 100.0
ESC_RATE_TOLERANCE = 0.05
# ESCs looked for in esc_status (its array has 8 slots).
MAX_ESCS = 8
# Finite stretches shorter than this are not fitted.
MIN_MOTOR_RUN_S = 0.25
# A median logged interval below this (above 10 kHz) is not a plausible PX4 logging
# rate; a crafted log could use it to make the resampling grids needlessly large.
MIN_LOGGED_INTERVAL_S = 1e-4
# Seconds cut from both ends of each in-air and each hover segment, leaving out
# takeoff and landing. Defined here, not in response.py, which imports this module.
TRIM_S = 2.0


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


def median_interval(t_s: np.ndarray, windows: list[Window]) -> float:
    """Median sample interval of t_s inside the windows; NaN when there is none."""
    inside = _inside(t_s, windows)
    steps = np.diff(t_s)[inside[1:] & inside[:-1]]
    return float(np.median(steps)) if steps.size else float("nan")


def implausible_interval(name: str, dt_s: float) -> str | None:
    """Why a topic's median interval is implausibly short, or None."""
    if dt_s >= MIN_LOGGED_INTERVAL_S:
        return None
    return (f"implausible logged interval ({name} median {dt_s:.2g} s, below "
            f"{MIN_LOGGED_INTERVAL_S:g} s: above {1e-3 / MIN_LOGGED_INTERVAL_S:g} kHz)")


def _inside(t_s: np.ndarray, windows: list[Window]) -> np.ndarray:
    mask = np.zeros(t_s.size, dtype=bool)
    for start, end in windows:
        mask |= (t_s >= start) & (t_s <= end)
    return mask


def _topic_data(ulog: Any, topic: str) -> dict | None:
    if topic not in {d.name for d in ulog.data_list}:
        return None
    return ulog.get_dataset(topic).data


def _series(data: dict, field: str, stamp: str = "timestamp") -> tuple[np.ndarray, np.ndarray]:
    """(t_s, values) of one field with repeated and backward stamps dropped."""
    return increasing_samples(np.asarray(data[stamp], dtype=float) * US_TO_S,
                              np.asarray(data[field], dtype=float))


def _param_value(params: dict[str, Any], name: str) -> float | None:
    """The parameter as a finite float, or None when absent or not a number."""
    try:
        value = float(params[name])
    except (KeyError, TypeError, ValueError):
        return None
    return value if math.isfinite(value) else None


def _finite_only(measurements: dict[str, float], errors: list[str]) -> Measured:
    bad = sorted(k for k, v in measurements.items() if not math.isfinite(v))
    if not bad:
        return measurements, errors
    kept = {k: v for k, v in measurements.items() if k not in bad}
    return kept, [*errors, f"non-finite values dropped: {', '.join(bad)}"]


def hover_windows(segments: list[FlightSegment]) -> list[Window]:
    """The HOVER segments longer than 2 * TRIM_S, each trimmed by TRIM_S at both ends."""
    return [(s.start_s + TRIM_S, s.end_s - TRIM_S) for s in segments
            if s.kind == SegmentKind.HOVER and s.end_s - s.start_s > 2 * TRIM_S]


# ---------------------------------------------------------------------------
# Hover
# ---------------------------------------------------------------------------


def _median_in(t_s: np.ndarray, x: np.ndarray, windows: list[Window]) -> float | None:
    values = x[_inside(t_s, windows) & np.isfinite(x)]
    return float(np.median(values)) if values.size else None


def _hover_estimate(ulog: Any, windows: list[Window]) -> float | None:
    data = _topic_data(ulog, TOPIC_HOVER_ESTIMATE)
    if data is None or "hover_thrust" not in data:
        return None
    t, x = _series(data, "hover_thrust")
    if "valid" in data:
        _, valid = _series(data, "valid")
        x = np.where(np.isfinite(valid) & (valid != 0), x, np.nan)  # NaN is not valid
    return _median_in(t, x, windows)


def _command_of(thrust: float, params: dict[str, Any]) -> tuple[float | None, str | None]:
    """(hover command, None) or (None, why not)."""
    factor = _param_value(params, "THR_MDL_FAC")
    if factor is None:
        return None, "no usable THR_MDL_FAC parameter (hover command unknown)"
    try:
        return hover_command(thrust, factor), None
    except ValueError as e:
        return None, f"hover command refused: {e}"


def hover_measurements(ulog: Any, params: dict[str, Any], windows: list[Window]) -> Measured:
    """The hover.* keys over the HOVER windows (at least one)."""
    measurements: dict[str, float] = {}
    errors: list[str] = []
    data = _topic_data(ulog, TOPIC_THRUST)
    thrust = None
    if data is not None and "xyz[2]" in data:
        t, z = _series(data, "xyz[2]")
        thrust = _median_in(t, -z, windows)
    if thrust is None:
        errors.append(f"no {TOPIC_THRUST} samples in the hover segments")
    else:
        measurements["hover.thrust"] = thrust
        command, why = _command_of(thrust, params)
        if command is not None:
            measurements["hover.command"] = command
        else:
            errors.append(why)
    for key, name in (("hover.thr_mdl_fac", "THR_MDL_FAC"), ("hover.mpc_thr_hover", "MPC_THR_HOVER")):
        value = _param_value(params, name)
        if value is not None:
            measurements[key] = value
    estimate = _hover_estimate(ulog, windows)
    if estimate is not None:
        measurements["hover.estimate"] = estimate
    return _finite_only(measurements, errors)


# ---------------------------------------------------------------------------
# In-flight gyro noise
# ---------------------------------------------------------------------------


def _runs_between_gaps(t_s: np.ndarray, max_gap_s: float) -> list[slice]:
    cuts = np.flatnonzero(np.diff(t_s) > max_gap_s) + 1
    edges = np.concatenate(([0], cuts, [t_s.size]))
    return [slice(a, b) for a, b in zip(edges[:-1], edges[1:])]


def _pooled_noise(columns: np.ndarray, runs: list[slice], dt_s: float,
                  cutoff_hz: float) -> float | None:
    """Std pooled over the runs long enough for the window, or None when none is."""
    total, count = 0.0, 0
    for run in runs:
        try:
            std = inflight_noise_std(columns[run], dt_s, cutoff_hz)
        except ValueError:
            continue
        total, count = total + std * std * (run.stop - run.start), count + (run.stop - run.start)
    return math.sqrt(total / count) if count else None


def inflight_noise_measurements(ulog: Any, windows: list[Window], cutoff_hz: float,
                                gap_intervals: float) -> Measured:
    """inflight_gyro_noise.<axis> and inflight_gyro_rate_hz over the hover windows."""
    data = _topic_data(ulog, TOPIC_RATE)
    if data is None:
        return {}, [f"no {TOPIC_RATE} for the in-flight noise"]
    stamp = "timestamp_sample" if "timestamp_sample" in data else "timestamp"
    t = np.asarray(data[stamp], dtype=float) * US_TO_S
    xyz = np.column_stack([np.asarray(data[f"xyz[{k}]"], dtype=float) for k in range(3)])
    t, xyz = increasing_samples(t, xyz)
    keep = _inside(t, windows) & np.all(np.isfinite(xyz), axis=1)
    t, xyz = t[keep], xyz[keep]
    if t.size < 2:
        return {}, ["no finite angular velocity samples in the hover segments"]
    dt = median_interval(t, windows)
    runs = _runs_between_gaps(t, gap_intervals * dt)
    measurements = {"inflight_gyro_rate_hz": sample_rate_hz(t)}
    errors: list[str] = []
    for k, axis in enumerate(AXES):
        std = _pooled_noise(xyz[:, k], runs, dt, cutoff_hz)
        if std is None:
            errors.append(f"hover segments too short for the in-flight noise at {cutoff_hz:g} Hz")
            return {}, errors
        measurements[f"inflight_gyro_noise.{axis}"] = std
    return _finite_only(measurements, errors)


def _unguarded(_group: str, measure: Callable[[], Measured]) -> Measured:
    return measure()


def hover_group(
    ulog: Any, params: dict[str, Any], segments: list[FlightSegment], cutoff_hz: float,
    gap_intervals: float,
    guard: Callable[[str, Callable[[], Measured]], Measured] = _unguarded,
) -> Measured:
    """The hover and in-flight noise keys; a log without a hover segment gets one error.

    ``guard(name, measure)`` runs each of the two sub-groups ("hover", "inflight noise"),
    so a failure in one leaves the other's keys.
    """
    windows = hover_windows(segments)
    if not windows:
        return {}, [f"no hover segment longer than {2 * TRIM_S:.0f} s "
                    "(hover values and in-flight gyro noise need one)"]
    hover_m, hover_e = guard("hover", lambda: hover_measurements(ulog, params, windows))
    noise_m, noise_e = guard("inflight noise", lambda: inflight_noise_measurements(
        ulog, windows, cutoff_hz, gap_intervals))
    return {**hover_m, **noise_m}, [*hover_e, *noise_e]


# ---------------------------------------------------------------------------
# Motor lag
# ---------------------------------------------------------------------------


def _rate_error(name: str, t_s: np.ndarray, windows: list[Window]) -> tuple[float, str | None]:
    """(median interval, None), or (nan, why the topic cannot be used) in the windows."""
    dt = median_interval(t_s, windows)
    if not math.isfinite(dt):
        return dt, f"ESC RPM: no {name} samples in the used in-air time"
    implausible = implausible_interval(name, dt)
    if implausible:
        return dt, f"ESC RPM: {implausible}"
    if 1.0 / dt < MIN_ESC_RPM_RATE_HZ * (1.0 - ESC_RATE_TOLERANCE):
        return dt, (f"ESC RPM: {name} logged at {1.0 / dt:.0f} Hz, need "
                    f"{MIN_ESC_RPM_RATE_HZ:.0f} Hz (a lag of tens of ms is not resolved)")
    return dt, None


def _used_windows(t_c: np.ndarray, cmd: np.ndarray, t_r: np.ndarray, rpm: np.ndarray,
                  grids: list[np.ndarray], gaps: tuple[float, float],
                  min_std: float) -> list[tuple[np.ndarray, np.ndarray]]:
    """(command, rpm) on each grid where both are finite somewhere and the command varies."""
    used = []
    for grid in grids:
        c = resample_on(t_c, cmd, grid, gaps[0])
        r = resample_on(t_r, rpm, grid, gaps[1])
        ok = np.isfinite(c) & np.isfinite(r)
        if ok.sum() >= 2 and float(np.std(c[ok])) > min_std:
            used.append((c, r))
    return used


def _finite_pairs(c: np.ndarray, r: np.ndarray, min_len: int) -> list[tuple[np.ndarray, np.ndarray]]:
    ok = np.isfinite(c) & np.isfinite(r)
    edges = np.flatnonzero(np.diff(np.concatenate(([0], ok.astype(np.int8), [0]))))
    return [(c[a:b], r[a:b]) for a, b in zip(edges[::2], edges[1::2]) if b - a >= min_len]


def _fit_esc(used: list[tuple[np.ndarray, np.ndarray]], dt_s: float,
             min_r2: float) -> PlantFit | str:
    """The fit of one ESC over its used windows, or why it is refused."""
    mean_rpm = float(np.mean(np.concatenate([r[np.isfinite(r) & np.isfinite(c)]
                                             for c, r in used])))
    min_len = max(2, int(MIN_MOTOR_RUN_S / dt_s))
    runs = [pair for c, r in used for pair in _finite_pairs(c, r - mean_rpm, min_len)]
    if not runs:
        return "no finite stretch long enough"
    try:
        fit = fit_first_order_plant_runs(runs, dt_s, MOTOR_DELAYS_S, MOTOR_TAUS_S)
    except ValueError as e:
        return f"fit failed ({e})"
    if not fit.gain > 0:
        return f"implausible gain ({fit.gain:.3g} rpm per unit command, need > 0)"
    if not fit.r2 >= min_r2:
        return f"low fit quality (R^2 {fit.r2:.3f} < {min_r2})"
    if fit.tau_s >= MOTOR_TAUS_S[-1] - 1e-9:
        return (f"motor lag at grid edge (time constant {fit.tau_s * 1e3:.0f} ms is the largest "
                "tried, the true lag is longer or the RPM does not follow the command)")
    return fit


def _live_escs(ulog: Any) -> tuple[dict, dict, list[int]] | str:
    """(esc_status data, actuator_motors data, ESC indices with RPM), or why there are none."""
    esc, motors = _topic_data(ulog, TOPIC_ESC), _topic_data(ulog, TOPIC_MOTORS)
    if esc is None:
        return f"ESC RPM: no {TOPIC_ESC} (no ESC RPM logged)"
    if motors is None:
        return f"ESC RPM: no {TOPIC_MOTORS} (the motor command)"
    ids = [i for i in range(MAX_ESCS)
           if f"esc[{i}].esc_rpm" in esc and f"control[{i}]" in motors]
    if not ids:
        return "ESC RPM: no esc[i].esc_rpm with a matching actuator_motors control[i]"
    live = [i for i in ids if np.any(np.asarray(esc[f"esc[{i}].esc_rpm"], dtype=float) != 0)]
    if not live:
        return "ESC RPM: all zero (the ESCs report no RPM)"
    return esc, motors, live


def _intervals(esc: dict, motors: dict, i: int, windows: list[Window]) -> tuple[float, float] | str:
    """(RPM interval, command interval) in the windows, or why a rate is too low."""
    dt_r, rate_error = _rate_error(TOPIC_ESC, _series(esc, f"esc[{i}].esc_rpm")[0], windows)
    dt_c, command_error = _rate_error(TOPIC_MOTORS, _series(motors, f"control[{i}]")[0], windows)
    return rate_error or command_error or (dt_r, dt_c)


def motor_lag_measurements(ulog: Any, windows: list[Window], min_cmd_std: float,
                           min_r2: float, gap_intervals: float) -> Measured:
    """motor_lag_s and motor_lag_r2: medians over the ESCs that fitted.

    Index i of esc_status is assumed to be motor i of actuator_motors (PX4 numbers
    both by output channel). ``windows`` are the in-air windows; an ESC is fitted on
    those where its command's std exceeds min_cmd_std. Both signals must be logged
    at MIN_ESC_RPM_RATE_HZ or more; the grid step and that rate check come from the
    first live ESC (the ESCs are assumed to log at one rate). A gap of more than
    gap_intervals median intervals in either is left empty.
    """
    inputs = _live_escs(ulog)
    if isinstance(inputs, str):
        return {}, [inputs]
    esc, motors, live = inputs
    dts = _intervals(esc, motors, live[0], windows)
    if isinstance(dts, str):
        return {}, [dts]
    dt_r, dt_c = dts
    grids = [start + np.arange(int((end - start) / dt_r) + 1) * dt_r for start, end in windows]
    gaps = (gap_intervals * dt_c, gap_intervals * dt_r)
    fits: list[PlantFit] = []
    reasons: list[str] = []
    for i in live:
        used = _used_windows(*_series(motors, f"control[{i}]"), *_series(esc, f"esc[{i}].esc_rpm"),
                             grids, gaps, min_cmd_std)
        outcome = (_fit_esc(used, dt_r, min_r2) if used
                   else f"motor command std not above {min_cmd_std} in any in-air window")
        if isinstance(outcome, PlantFit):
            fits.append(outcome)
        else:
            reasons.append(f"esc[{i}]: {outcome}")
    if not fits:
        return {}, [f"ESC RPM: no ESC fitted ({'; '.join(reasons)})"]
    return _finite_only({"motor_lag_s": float(np.median([f.lag_s for f in fits])),
                         "motor_lag_r2": float(np.median([f.r2 for f in fits]))}, [])


# ---------------------------------------------------------------------------
# Rates, parameters, cell count
# ---------------------------------------------------------------------------


def rate_measurements(ulog: Any) -> Measured:
    """rate.torque_setpoint_hz and rate.angular_velocity_hz from each topic's timestamp."""
    measurements: dict[str, float] = {}
    errors: list[str] = []
    for key, topic in (("rate.torque_setpoint_hz", TOPIC_TORQUE),
                       ("rate.angular_velocity_hz", TOPIC_RATE)):
        data = _topic_data(ulog, topic)
        if data is None:
            errors.append(f"no {topic} for its rate")
            continue
        try:
            measurements[key] = sample_rate_hz(_series(data, "timestamp")[0])
        except ValueError as e:
            errors.append(f"{topic}: rate unavailable ({e})")
    return _finite_only(measurements, errors)


def param_measurements(params: dict[str, Any]) -> Measured:
    """param.<name> for each of RECORDED_PARAMS the log has."""
    values = {f"param.{name}": _param_value(params, name) for name in RECORDED_PARAMS}
    return {k: v for k, v in values.items() if v is not None}, []


def cell_count_measurements(ulog: Any) -> Measured:
    """cell_count: the first positive battery_status.cell_count; absent without one."""
    data = _topic_data(ulog, TOPIC_BATTERY)
    if data is None or "cell_count" not in data:
        return {}, []
    counts = np.asarray(data["cell_count"], dtype=float)
    positive = counts[np.isfinite(counts) & (counts > 0)]
    return ({"cell_count": float(positive[0])} if positive.size else {}), []
