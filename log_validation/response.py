"""Response pass: control effectiveness and lag per axis from real quadrotor logs.

Usage:
    python -m log_validation.response data/downloaded/ -o response.json

For roll, pitch and yaw the pass fits the angular acceleration to the rate
controller's torque setpoint passed through a pure delay and a first-order lag
(``sysid.fit_first_order_plant_runs``; delays 0 to 40 ms and time constants 0 to
60 ms, 1 ms steps) by two-stage least squares (see "Closed loop" below) and reports
the gain (``effectiveness.<axis>``, rad/s^2 per unit of normalized torque), the
delay plus time constant (``lag_s.<axis>``), the R^2 projected on the instruments
(``fit_r2.<axis>``), the instrument strength (``first_stage_r2.<axis>``) and the
share of in-air time used (``used_fraction.<axis>``). Inputs:
``vehicle_torque_setpoint.xyz[k]`` on its ``timestamp`` (applied when published),
``vehicle_angular_velocity.xyz_derivative[k]`` on its ``timestamp_sample`` and
``vehicle_rates_setpoint`` roll, pitch and yaw on their ``timestamp``, all resampled
at the median angular-velocity interval inside the in-air segments of
``flight_segments.detect_flight_segments``, each trimmed by 2 s at both ends so
takeoff and landing are left out. ``bat_scale_en`` reports MC_BAT_SCALE_EN (1.0 or
0.0, absent without the parameter): with it on, PX4 scales the published torque by
the battery voltage, so the effectiveness is per unit of scaled torque.

Filter delay (``filter_delay_s``, and ``lag_minus_filter_s.<axis>`` = lag minus it).
Source: PX4-Autopilot ``src/modules/sensors/vehicle_angular_velocity/
VehicleAngularVelocity.cpp`` and ``.hpp`` at tag v1.14.0; the chain is the same at
v1.13.0, v1.15.0 and v1.16.0. ``FilterAngularVelocity()`` filters the gyro samples
in place: the ESC RPM and FFT dynamic notches, the IMU_GYRO_NF0/NF1 static notches,
then the second-order ``LowPassFilter2p`` at IMU_GYRO_CUTOFF.
``FilterAngularAcceleration()`` then differentiates those same, already filtered,
samples and smooths the result with a first-order ``AlphaFilter`` at
IMU_DGYRO_CUTOFF; ``CalibrateAndPublish()`` publishes it as ``xyz_derivative``. So
filter_delay_s = sqrt(2) / (2 pi IMU_GYRO_CUTOFF) + 1 / (2 pi IMU_DGYRO_CUTOFF),
a cutoff <= 0 counting as 0 (disabled). Not counted: the notches, whose delay below
their centre frequency is bandwidth / (2 pi centre^2), under the 1 ms grid step at
usual settings (the static ones are off by default), while the dynamic ones follow
ESC RPM or the FFT in flight, which the parameters do not give; and the backward
difference, half a raw gyro interval (0.5 ms at 1 kHz, less above).

Older logs without ``xyz_derivative``: the angular acceleration is the central
difference of ``vehicle_angular_velocity.xyz`` on the resampled grid (no delay), so
filter_delay_s holds the IMU_GYRO_CUTOFF low-pass only. The measurement
``angular_accel_from_rate`` records the source: 1.0 for this fallback, 0.0 for
``xyz_derivative``.

Closed loop. In PX4 v1.14.0 ``src/lib/rate_control/rate_control.cpp``
(``RateControl::update``) computes the torque setpoint as P * rate error + integral
- D * angular_accel + FF * rate setpoint, and ``mc_rate_control`` passes
``vehicle_angular_velocity.xyz_derivative`` as angular_accel: the very signal fitted
here. So the setpoint contains -D times the measured disturbance at zero delay, and a
plain least-squares fit is pulled toward low or negative gains and short lags. The
fit therefore uses the axis's rate setpoint history (``INSTRUMENT_LAGS_S``, 0 to
80 ms) as instruments: it drives the torque through the P and FF terms but comes from
the pilot or the attitude loop, not from this instant's disturbance. At each grid
point the delayed, low-passed torque is projected on those instruments and the
angular acceleration regressed on the projection; the point with the highest R^2
projected on the instruments is kept. A log without ``vehicle_rates_setpoint`` gets
no fit. Gates per axis: excitation (torque std), instrument strength
(``first_stage_r2``, "weak instrument"), fit quality (``fit_r2``), and two
plausibility gates that no real plant can fail: a gain <= 0 ("implausible gain") or
a lag below filter_delay_s ("lag below filter delay") gives no estimate.

Only real quadrotors are analyzed: simulation logs (``hardware.is_simulation``) and
logs whose MAV_TYPE (or, without it, CA_ROTOR_COUNT) is not a quadrotor get no
measurements and an error naming why.
"""

from __future__ import annotations

import argparse
import sys
from collections.abc import Callable
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import numpy as np
from pyulog import ULog

# The noise pass's airframe rule (SYS_AUTOSTART, None when absent or 0), so both outputs agree.
from .batch_analyze import _airframe_id
from .flight_segments import detect_flight_segments
from .hardware import extract_hardware_info, is_simulation
from .manifest import DEFAULT_MAX_PER_VEHICLE, ManifestError, add_selection_args, select_logs
from .outputs import PrivateDataError, forbidden_strings, write_json
from .parallel import run_per_log
from .sysid import (
    PlantFit,
    butterworth2_group_delay_s,
    first_order_group_delay_s,
    fit_first_order_plant_runs,
    increasing_samples,
    resample_on,
)
from .ulog_loader import US_TO_S

AXES = ("roll", "pitch", "yaw")
TOPIC_TORQUE = "vehicle_torque_setpoint"
TOPIC_RATE = "vehicle_angular_velocity"
TOPIC_RATES_SP = "vehicle_rates_setpoint"
RATES_SP_FIELDS = ("roll", "pitch", "yaw")
# The topics the pass reads: the signals, the instrument and what detect_flight_segments needs.
RESPONSE_TOPICS = (TOPIC_TORQUE, TOPIC_RATE, TOPIC_RATES_SP, "vehicle_status",
                   "vehicle_land_detected", "vehicle_local_position")

# Seconds cut from both ends of each in-air segment, leaving out takeoff and landing.
TRIM_S = 2.0
# The fit grid: delays 0 to 40 ms and first-order time constants 0 to 60 ms, 1 ms steps.
FIT_DELAYS_S = tuple(i * 1e-3 for i in range(41))
FIT_TAUS_S = tuple(i * 1e-3 for i in range(61))
# Finite stretches shorter than this are not used (too short to hold a 40 ms delay well).
MIN_RUN_S = 0.25
# The instrument: the axis's rate setpoint at these lags (its last 80 ms of history).
INSTRUMENT_LAGS_S = (0.0, 0.005, 0.010, 0.020, 0.040, 0.080)
# Every signal must be logged at least this often: the rate loop runs at 250 Hz or more,
# and a fit of a lag of tens of ms needs samples a few ms apart. PX4's default logger
# profile (torque setpoint at 50 Hz) falls below it.
MIN_LOGGED_RATE_HZ = 200.0
# Accepted shortfall of a logged median rate below MIN_LOGGED_RATE_HZ: PX4 publishes the
# rate setpoint at about 200 Hz and scheduling jitter often logs it at 198 or 199 Hz.
LOGGED_RATE_TOLERANCE = 0.05
# Vehicle type: MAV_TYPE 2 is MAV_TYPE_QUADROTOR; without MAV_TYPE, four rotors.
MAV_TYPE_QUADROTOR = 2
QUADROTOR_ROTOR_COUNT = 4

ERROR_SIMULATION = "simulation log skipped (SITL/HIL)"
ERROR_NO_TORQUE = f"no {TOPIC_TORQUE} (firmware before control allocation)"
ERROR_WORKER_DIED = "worker process died"


@dataclass(frozen=True)
class ResponseGates:
    """Per-axis acceptance gates (provisional; Task 13 sets them from the sample run)."""

    min_torque_std: float = 0.02  # excitation: torque setpoint std over the used time
    min_r2: float = 0.5  # fit quality
    noise_cutoff_hz: float = 20.0  # in-flight noise cut-off (Task 8)
    min_motor_cmd_std: float = 0.02  # motor command variation for motor lag (Task 8)
    min_first_stage_r2: float = 0.05  # instrument strength: R^2 of the torque on it


@dataclass(frozen=True)
class ResponseResult:
    """Response measurements of one log, keyed by quantity and axis."""

    log_id: str
    vehicle_id: str | None
    airframe_id: int | None
    hardware: dict
    measurements: dict[str, float]
    errors: list[str]


@dataclass(frozen=True)
class _Header:
    params: dict
    airframe_id: int | None
    hardware: dict
    simulation: bool
    errors: tuple[str, ...]


@dataclass(frozen=True)
class _Signals:
    """The signals on the uniform grid of each used window, one array per window."""

    dt_s: float
    torque: tuple[np.ndarray, ...]
    accel: tuple[np.ndarray, ...]
    instruments: tuple[np.ndarray, ...]  # (n, 3, len(INSTRUMENT_LAGS_S)) per window
    accel_from_rate: bool


# ---------------------------------------------------------------------------
# Header: parameters, airframe, hardware, and the refusals
# ---------------------------------------------------------------------------


def _read_header(ulog: Any) -> _Header:
    errors: list[str] = []
    try:
        params = dict(ulog.initial_parameters)
    except Exception as e:
        params = {}
        errors.append(f"parameters unavailable: {type(e).__name__}")
    try:
        hw = extract_hardware_info(ulog.msg_info_dict, params)
        hardware, simulation = asdict(hw), is_simulation(hw)
    except Exception as e:
        hardware, simulation = {}, False
        errors.append(f"hardware extraction failed: {type(e).__name__}")
    return _Header(params, _airframe_id(params), hardware, simulation, tuple(errors))


def _param_text(value: Any) -> str:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return str(value)
    return str(int(number)) if number.is_integer() else str(number)


def _vehicle_type_error(params: dict[str, Any]) -> str | None:
    """None for a quadrotor: MAV_TYPE == 2, or without MAV_TYPE, CA_ROTOR_COUNT == 4."""
    mav_type = params.get("MAV_TYPE")
    if mav_type is not None:
        if _param_text(mav_type) == str(MAV_TYPE_QUADROTOR):
            return None
        return f"not a quadrotor (MAV_TYPE={_param_text(mav_type)})"
    rotors = params.get("CA_ROTOR_COUNT")
    if rotors is not None and _param_text(rotors) == str(QUADROTOR_ROTOR_COUNT):
        return None
    return f"not a quadrotor (MAV_TYPE=absent, CA_ROTOR_COUNT={_param_text(rotors)})"


def _refusal(ulog: Any, header: _Header) -> str | None:
    """Why this log gets no response measurements at all, or None."""
    if not header.hardware:
        return "simulation status unknown (no hardware information)"
    if header.simulation:
        return ERROR_SIMULATION
    vehicle_error = _vehicle_type_error(header.params)
    if vehicle_error:
        return vehicle_error
    topics = {d.name for d in ulog.data_list}
    if TOPIC_TORQUE not in topics:
        return ERROR_NO_TORQUE
    if TOPIC_RATE not in topics:
        return f"no {TOPIC_RATE}"
    if TOPIC_RATES_SP not in topics:
        return f"no {TOPIC_RATES_SP} (the instrument of the closed-loop fit)"
    return None


# ---------------------------------------------------------------------------
# Signals: in-air windows, filter delay, resampled torque and angular acceleration
# ---------------------------------------------------------------------------


def _in_air_windows(ulog: Any) -> tuple[list[tuple[float, float]], float]:
    """(used windows, total in-air seconds): each in-air segment trimmed by TRIM_S at both ends."""
    segments = detect_flight_segments(ulog)
    in_air_s = float(sum(s.end_s - s.start_s for s in segments))
    windows = [(s.start_s + TRIM_S, s.end_s - TRIM_S) for s in segments
               if s.end_s - s.start_s > 2 * TRIM_S]
    return windows, in_air_s


def _filter_delay(params: dict[str, Any], has_derivative: bool) -> tuple[float | None, str | None]:
    """(filter_delay_s, None), or (None, error) when a cutoff parameter the path needs is absent."""
    names = ("IMU_GYRO_CUTOFF", "IMU_DGYRO_CUTOFF") if has_derivative else ("IMU_GYRO_CUTOFF",)
    missing = [n for n in names if params.get(n) is None]
    if missing:
        return None, f"no {', '.join(missing)} parameter (filter delay unknown)"
    delay = butterworth2_group_delay_s(float(params["IMU_GYRO_CUTOFF"]))
    if has_derivative:
        delay += first_order_group_delay_s(float(params["IMU_DGYRO_CUTOFF"]))
    return delay, None


def _xyz(field: str) -> tuple[str, ...]:
    return tuple(f"{field}[{k}]" for k in range(3))


def _vector(data: dict, fields: tuple[str, ...], stamp: str) -> tuple[np.ndarray, np.ndarray]:
    """(t_s, (n, 3) values) of three fields on the stamp field, repeated/backward stamps dropped."""
    t_s = np.asarray(data[stamp], dtype=float) * US_TO_S
    values = np.column_stack([np.asarray(data[f], dtype=float) for f in fields])
    return increasing_samples(t_s, values)


def _median_interval(t_s: np.ndarray, windows: list[tuple[float, float]]) -> float:
    inside = np.zeros(t_s.size, dtype=bool)
    for start, end in windows:
        inside |= (t_s >= start) & (t_s <= end)
    steps = np.diff(t_s)[inside[1:] & inside[:-1]]
    return float(np.median(steps)) if steps.size else float("nan")


def _on_grid(t_s: np.ndarray, values: np.ndarray, grid: np.ndarray) -> np.ndarray:
    return np.column_stack([resample_on(t_s, values[:, k], grid) for k in range(3)])


def instrument_matrix(
    t_s: np.ndarray, x: np.ndarray, grid_s: np.ndarray,
    lags_s: tuple[float, ...] = INSTRUMENT_LAGS_S,
) -> np.ndarray:
    """(len(grid_s), len(lags_s)): x interpolated at grid_s - lag for each lag (its history).

    NaN where grid_s - lag falls outside the span of x.
    """
    grid = np.asarray(grid_s, dtype=float)
    return np.column_stack([resample_on(t_s, x, grid - lag) for lag in lags_s])


def _logging_error(intervals: dict[str, float]) -> str | None:
    """Why the signals cannot be fitted as logged (median intervals by topic), or None."""
    for topic, dt in intervals.items():
        if not np.isfinite(dt):
            return f"no {topic} samples in the used in-air time"
    floor_hz = MIN_LOGGED_RATE_HZ * (1.0 - LOGGED_RATE_TOLERANCE)
    if max(intervals.values()) * floor_hz > 1.0:
        rates = ", ".join(f"{topic} at {1 / dt:.0f} Hz" for topic, dt in intervals.items())
        return f"not logged at the rate controller's rate ({rates}; need {floor_hz:.0f} Hz)"
    return None


def _resampled(ulog: Any, windows: list[tuple[float, float]]) -> _Signals | str:
    """The signals on each window's grid, or an error when they are logged too sparsely."""
    rate_data = ulog.get_dataset(TOPIC_RATE).data
    stamp = "timestamp_sample" if "timestamp_sample" in rate_data else "timestamp"
    from_rate = "xyz_derivative[0]" not in rate_data
    t_y, y = _vector(rate_data, _xyz("xyz" if from_rate else "xyz_derivative"), stamp)
    t_u, u = _vector(ulog.get_dataset(TOPIC_TORQUE).data, _xyz("xyz"), "timestamp")
    t_z, z = _vector(ulog.get_dataset(TOPIC_RATES_SP).data, RATES_SP_FIELDS, "timestamp")
    intervals = {topic: _median_interval(t, windows)
                 for topic, t in ((TOPIC_TORQUE, t_u), (TOPIC_RATE, t_y), (TOPIC_RATES_SP, t_z))}
    error = _logging_error(intervals)
    if error:
        return error
    dt = intervals[TOPIC_RATE]
    grids = [start + np.arange(int((end - start) / dt) + 1) * dt for start, end in windows]
    accel = tuple(_on_grid(t_y, y, g) for g in grids)
    if from_rate:
        accel = tuple(np.gradient(a, dt, axis=0) if len(a) > 1 else a * np.nan for a in accel)
    instruments = tuple(np.stack([instrument_matrix(t_z, z[:, k], g) for k in range(3)], axis=1)
                        for g in grids)
    return _Signals(dt, tuple(_on_grid(t_u, u, g) for g in grids), accel, instruments, from_rate)


# ---------------------------------------------------------------------------
# Per-axis measurement
# ---------------------------------------------------------------------------

_Run = tuple[np.ndarray, np.ndarray, np.ndarray]


def _finite_runs(u: np.ndarray, y: np.ndarray, z: np.ndarray, min_len: int) -> list[_Run]:
    """The stretches of at least min_len samples where u, y and every instrument are finite."""
    ok = np.isfinite(u) & np.isfinite(y) & np.all(np.isfinite(z), axis=1)
    edges = np.flatnonzero(np.diff(np.concatenate(([0], ok.astype(np.int8), [0]))))
    return [(u[a:b], y[a:b], z[a:b]) for a, b in zip(edges[::2], edges[1::2])
            if b - a >= min_len]


def _axis_runs(signals: _Signals, k: int) -> list[_Run]:
    min_len = max(2, int(MIN_RUN_S / signals.dt_s))
    return [run for u, y, z in zip(signals.torque, signals.accel, signals.instruments)
            for run in _finite_runs(u[:, k], y[:, k], z[:, k, :], min_len)]


def _implausibility(axis: str, fit: PlantFit, filter_delay_s: float) -> str | None:
    """Why a fit no real plant could produce is refused, or None (see the module docstring)."""
    if not fit.gain > 0:
        return f"{axis}: implausible gain ({fit.gain:.3g} rad/s^2 per unit torque, need > 0)"
    if not fit.lag_s >= filter_delay_s:
        return (f"{axis}: lag below filter delay (lag {fit.lag_s * 1e3:.1f} ms "
                f"< filter delay {filter_delay_s * 1e3:.1f} ms)")
    return None


def _fit_gate(axis: str, fit: PlantFit, gates: ResponseGates) -> str | None:
    """Why a fit is too weak to report, or None."""
    if not fit.first_stage_r2 >= gates.min_first_stage_r2:
        return (f"{axis}: weak instrument (first-stage R^2 {fit.first_stage_r2:.3f} "
                f"< {gates.min_first_stage_r2})")
    if not fit.r2 >= gates.min_r2:
        return f"{axis}: low fit quality (R^2 {fit.r2:.3f} < {gates.min_r2})"
    return None


def _axis_response(
    axis: str, runs: list[_Run], signals: _Signals,
    in_air_s: float, filter_delay_s: float, gates: ResponseGates,
) -> tuple[dict[str, float], list[str]]:
    """Measurements and errors of one axis; a gated axis has no effectiveness or lag."""
    used_s = sum(len(run[0]) for run in runs) * signals.dt_s
    measurements = {f"used_fraction.{axis}": used_s / in_air_s if in_air_s > 0 else 0.0}
    if not runs:
        return measurements, [f"{axis}: no in-air samples with all signals finite"]
    torque_std = float(np.std(np.concatenate([run[0] for run in runs])))
    if not torque_std >= gates.min_torque_std:
        return measurements, [f"{axis}: low excitation (torque setpoint std {torque_std:.4f} "
                              f"< {gates.min_torque_std})"]
    try:
        fit = fit_first_order_plant_runs(runs, signals.dt_s, FIT_DELAYS_S, FIT_TAUS_S)
    except ValueError as e:
        return measurements, [f"{axis}: fit failed ({e})"]
    measurements[f"fit_r2.{axis}"] = fit.r2
    measurements[f"first_stage_r2.{axis}"] = fit.first_stage_r2
    refusal = _fit_gate(axis, fit, gates) or _implausibility(axis, fit, filter_delay_s)
    if refusal:
        return measurements, [refusal]
    measurements[f"effectiveness.{axis}"] = fit.gain
    measurements[f"lag_s.{axis}"] = fit.lag_s
    measurements[f"lag_minus_filter_s.{axis}"] = fit.lag_s - filter_delay_s
    return measurements, []


def _battery_scaling(params: dict[str, Any]) -> dict[str, float]:
    """bat_scale_en: 1.0 when MC_BAT_SCALE_EN scales the published torque, 0.0 when not."""
    value = params.get("MC_BAT_SCALE_EN")
    if value is None:
        return {}
    return {"bat_scale_en": 0.0 if float(value) == 0 else 1.0}


def _effectiveness_and_lag(
    ulog: Any, params: dict[str, Any], gates: ResponseGates
) -> tuple[dict[str, float], list[str]]:
    windows, in_air_s = _in_air_windows(ulog)
    if not windows:
        return {}, [f"no in-air segment longer than {2 * TRIM_S:.0f} s"]
    has_derivative = "xyz_derivative[0]" in ulog.get_dataset(TOPIC_RATE).data
    filter_delay_s, delay_error = _filter_delay(params, has_derivative)
    if delay_error:
        return {}, [delay_error]
    signals = _resampled(ulog, windows)
    if isinstance(signals, str):
        return {}, [signals]
    measurements = {"filter_delay_s": filter_delay_s,
                    "angular_accel_from_rate": 1.0 if signals.accel_from_rate else 0.0}
    errors: list[str] = []
    for k, axis in enumerate(AXES):
        axis_m, axis_e = _axis_response(axis, _axis_runs(signals, k), signals, in_air_s,
                                        filter_delay_s, gates)
        measurements.update(axis_m)
        errors.extend(axis_e)
    return measurements, errors


# ---------------------------------------------------------------------------
# One log
# ---------------------------------------------------------------------------


def analyze_response(
    ulog: Any, log_id: str, vehicle_id: str | None, gates: ResponseGates
) -> ResponseResult:
    """Response measurements of one parsed log; any failure becomes an error, never a raise."""
    header = _read_header(ulog)

    def result(measurements: dict[str, float], errors: list[str]) -> ResponseResult:
        return ResponseResult(log_id, vehicle_id, header.airframe_id, header.hardware,
                              measurements, [*header.errors, *errors])

    try:
        refusal = _refusal(ulog, header)
        if refusal:
            return result({}, [refusal])
        measurements, errors = _effectiveness_and_lag(ulog, header.params, gates)
        return result({**_battery_scaling(header.params), **measurements}, errors)
    except Exception as e:
        return result({}, [f"analysis failed: {type(e).__name__}: {e}"])


def load_response_topics(path: Path) -> ULog:
    """Parse only the topics the pass reads; the header (parameters, info) is always read."""
    return ULog(str(path), message_name_filter_list=list(RESPONSE_TOPICS))


def analyze_response_file(
    path: Path,
    vehicle_id: str | None,
    gates: ResponseGates,
    loader: Callable[[Path], Any] = load_response_topics,
) -> ResponseResult:
    """Load one log and analyze it; a load failure is reported by exception type only
    (its message often holds the file path)."""
    log_id = Path(path).stem
    try:
        ulog = loader(path)
    except Exception as e:
        return ResponseResult(log_id, vehicle_id, None, {}, {},
                              [f"load failed: {type(e).__name__}"])
    return analyze_response(ulog, log_id, vehicle_id, gates)


# ---------------------------------------------------------------------------
# Batch and CLI
# ---------------------------------------------------------------------------

_Task = tuple[Path, str | None, ResponseGates, Callable[[Path], Any]]


def _response_task(task: _Task) -> ResponseResult:
    path, vehicle_id, gates, loader = task
    return analyze_response_file(path, vehicle_id, gates, loader)


def _died_result(task: _Task) -> ResponseResult:
    path, vehicle_id, _, _ = task
    return ResponseResult(Path(path).stem, vehicle_id, None, {}, {}, [ERROR_WORKER_DIED])


def response_batch(
    log_dir: Path,
    output_path: Path | None,
    manifest_path: Path | None = None,
    max_per_vehicle: int = DEFAULT_MAX_PER_VEHICLE,
    jobs: int = 1,
    gates: ResponseGates = ResponseGates(),
    loader: Callable[[Path], Any] = load_response_topics,
) -> list[ResponseResult]:
    """Analyze the selected logs of log_dir; results sorted by log ID, output guarded."""
    selection = select_logs(log_dir, manifest_path, max_per_vehicle)
    tasks = [(p, selection.vehicle_of.get(p.stem), gates, loader) for p in selection.paths]
    results = sorted(run_per_log(_response_task, tasks, jobs, _died_result),
                     key=lambda r: r.log_id)
    if output_path is not None:
        write_json(output_path, [asdict(r) for r in results], forbidden_strings(log_dir))
        print(f"Wrote {len(results)} response results to {output_path}", file=sys.stderr)
    return results


def _build_parser() -> argparse.ArgumentParser:
    defaults = ResponseGates()
    parser = argparse.ArgumentParser(
        prog="python -m log_validation.response",
        description="Measure control effectiveness and lag per axis from real quadrotor logs.",
        epilog="Exit status: 0 done, 2 error.",
    )
    parser.add_argument("log_dir", type=Path, help="directory containing .ulg files")
    parser.add_argument("-o", "--output", type=Path, required=True, help="output JSON file")
    add_selection_args(parser)
    gates = parser.add_argument_group("gates")
    gates.add_argument("--min-torque-std", type=float, default=defaults.min_torque_std,
                       help="excitation gate: torque setpoint std per axis "
                            f"(default {defaults.min_torque_std})")
    gates.add_argument("--min-r2", type=float, default=defaults.min_r2,
                       help=f"fit quality gate: R^2 per axis (default {defaults.min_r2})")
    gates.add_argument("--noise-cutoff-hz", type=float, default=defaults.noise_cutoff_hz,
                       help=f"in-flight noise cut-off in Hz (default {defaults.noise_cutoff_hz})")
    gates.add_argument("--min-motor-cmd-std", type=float, default=defaults.min_motor_cmd_std,
                       help="motor lag gate: motor command std "
                            f"(default {defaults.min_motor_cmd_std})")
    gates.add_argument("--min-first-stage-r2", type=float, default=defaults.min_first_stage_r2,
                       help="instrument strength gate: first-stage R^2 per axis "
                            f"(default {defaults.min_first_stage_r2})")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    if not args.log_dir.is_dir():
        print(f"Error: {args.log_dir} is not a directory", file=sys.stderr)
        return 2
    gates = ResponseGates(args.min_torque_std, args.min_r2, args.noise_cutoff_hz,
                          args.min_motor_cmd_std, args.min_first_stage_r2)
    try:
        response_batch(args.log_dir, args.output, args.manifest, args.max_per_vehicle,
                       args.jobs, gates)
    except (ManifestError, PrivateDataError, ValueError, OSError) as e:
        print(f"Error: {e}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
