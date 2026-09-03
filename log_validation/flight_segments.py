"""Detect armed/in-air flight segments and classify their dynamics.

Segment detection priority (most reliable first):
  1. ``vehicle_status.arming_state`` (2 = armed) **and**
     ``vehicle_land_detected.landed`` (0 = in-air).  Both topics present →
     armed-and-not-landed windows.
  2. ``vehicle_status.arming_state`` alone (both topics present in most logs).
  3. ``vehicle_local_position.z`` altitude threshold when neither status topic
     is available (rare, older firmware).

Sub-segment classification uses thresholds applied to interpolated
``vehicle_local_position`` (vz, vx, vy) and the magnitude of
``vehicle_angular_acceleration.xyz``.  The thresholds are named constants with
rationale comments so they are easy to review and adjust.

All public return types are frozen dataclasses (immutable).

Simulation logs MUST NOT be passed to ``detect_flight_segments``; the caller
(or ``topic_inventory``) is responsible for calling
``hardware.is_simulation(hw)`` first.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

import numpy as np

from .ulog_loader import Series, series, topic_names

# ---------------------------------------------------------------------------
# Thresholds — all named, all with rationale
# ---------------------------------------------------------------------------

# PX4 arming_state enum value for the ARMED state.
# (PX4-Autopilot/src/modules/commander/state_machine_helper.cpp)
#   1 = STANDBY, 2 = ARMED, 3 = ARMED_ERROR, 4 = STANDBY_ERROR, 5 = REBOOT, 6 = IN_AIR_RESTORE
ARMING_STATE_ARMED = 2

# vehicle_land_detected.landed == 0 means the vehicle is in the air.
LANDED_VALUE_AIRBORNE = 0

# Minimum contiguous in-air duration (seconds) for a window to be kept.
# Shorter snippets are artefacts of rapid arming/disarming during pre-flight
# checks, not real flights.
MIN_FLIGHT_DURATION_S = 2.0

# Altitude threshold (NED frame, so z < 0 means above ground) used as a
# last-resort fallback when vehicle_status is absent.  0.5 m gives ~20 cm
# clearance above noise in z_valid estimates.
ALTITUDE_THRESHOLD_NED_M = -0.5  # z < this → airborne in NED

# Climb / descent classification on vz (NED, so positive = down).
# 0.5 m/s corresponds to a gentle intentional manoeuvre well above sensor
# noise (typical vz noise floor ~0.05–0.10 m/s).
VZ_CLIMB_THRESHOLD_MS = -0.5  # vz < this → climbing (NED: up = negative)
VZ_DESCENT_THRESHOLD_MS = 0.5  # vz > this → descending

# Horizontal speed threshold for "translation" classification.
# 1.5 m/s separates loitering / position-hold (which shows very low speed)
# from deliberate lateral flight.  Below this speed the drone is likely
# hovering with small position corrections.
HORIZ_SPEED_TRANSLATION_MS = 1.5

# Angular acceleration magnitude threshold for "aggressive" manoeuvres.
# Typical calm hover: |α| ~ 2–5 rad/s².  Aggressive acro: >> 20 rad/s².
# 15 rad/s² is a conservative threshold that catches rapid flips/rolls while
# ignoring normal attitude corrections.
ANGULAR_ACCEL_AGGRESSIVE_RADS2 = 15.0


# ---------------------------------------------------------------------------
# Public types
# ---------------------------------------------------------------------------


class SegmentKind(str, Enum):
    """Dynamics classification for an in-flight sub-segment."""

    HOVER = "hover"
    CLIMB = "climb"
    DESCENT = "descent"
    TRANSLATION = "translation"
    AGGRESSIVE = "aggressive"
    UNKNOWN = "unknown"  # local-position data unavailable for this window


@dataclass(frozen=True)
class FlightSegment:
    """An in-flight sub-segment with start/end times and dynamics class.

    ``start_s`` and ``end_s`` are seconds since log boot (same epoch as
    ``Series.t_s``).  ``n_samples`` counts the local-position samples inside
    the window (used by callers to discard data-sparse segments).
    """

    start_s: float
    end_s: float
    kind: SegmentKind
    n_samples: int


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _interp_at(t_query: np.ndarray, t_src: np.ndarray, vals: np.ndarray) -> np.ndarray:
    """Linear-interpolate *vals* (sampled at *t_src*) onto *t_query* times."""
    return np.interp(t_query, t_src, vals)


def _armed_mask_from_status(
    t_query: np.ndarray, ulog, topic_map: dict[str, int]
) -> np.ndarray | None:
    """Return boolean mask of armed + in-air samples, or None if topics absent."""
    if "vehicle_status" not in topic_map:
        return None

    arming = series(ulog, "vehicle_status", "arming_state")
    armed_at_query = _interp_at(t_query, arming.t_s, arming.values)
    armed = armed_at_query >= ARMING_STATE_ARMED  # covers ARMED and ARMED_ERROR

    if "vehicle_land_detected" not in topic_map:
        return armed  # best effort without land detection

    landed = series(ulog, "vehicle_land_detected", "landed")
    landed_at_query = _interp_at(t_query, landed.t_s, landed.values)
    # landed == 0 means in-air; include maybe_landed (0) as airborne too
    airborne = landed_at_query < 1.0

    return armed & airborne


def _armed_mask_from_altitude(t_query: np.ndarray, z: np.ndarray) -> np.ndarray:
    """Altitude-based fallback: z < ALTITUDE_THRESHOLD_NED_M in NED frame."""
    return z < ALTITUDE_THRESHOLD_NED_M


def _find_contiguous_windows(
    mask: np.ndarray, t: np.ndarray
) -> list[tuple[float, float]]:
    """Return [(start_s, end_s)] for each True run longer than MIN_FLIGHT_DURATION_S."""
    if not np.any(mask):
        return []

    padded = np.concatenate([[False], mask, [False]])
    diff = np.diff(padded.astype(int))
    starts_idx = np.where(diff == 1)[0]  # index into *mask* (pre-pad removed)
    ends_idx = np.where(diff == -1)[0]  # exclusive end

    windows: list[tuple[float, float]] = []
    for s, e in zip(starts_idx, ends_idx):
        t_start = float(t[s])
        t_end = float(t[min(e, len(t) - 1)])
        if t_end - t_start >= MIN_FLIGHT_DURATION_S:
            windows.append((t_start, t_end))
    return windows


def _classify_sample(
    vz: float,
    vx: float,
    vy: float,
    ang_accel_mag: float | None,
) -> SegmentKind:
    """Classify a single sample based on velocity and angular acceleration."""
    if ang_accel_mag is not None and ang_accel_mag >= ANGULAR_ACCEL_AGGRESSIVE_RADS2:
        return SegmentKind.AGGRESSIVE

    horiz_speed = (vx**2 + vy**2) ** 0.5

    if vz < VZ_CLIMB_THRESHOLD_MS:
        return SegmentKind.CLIMB
    if vz > VZ_DESCENT_THRESHOLD_MS:
        return SegmentKind.DESCENT
    if horiz_speed > HORIZ_SPEED_TRANSLATION_MS:
        return SegmentKind.TRANSLATION
    return SegmentKind.HOVER


def _majority_kind(kinds: list[SegmentKind]) -> SegmentKind:
    """Return the most common SegmentKind in *kinds*."""
    if not kinds:
        return SegmentKind.UNKNOWN
    counts: dict[SegmentKind, int] = {}
    for k in kinds:
        counts[k] = counts.get(k, 0) + 1
    return max(counts, key=lambda k: counts[k])


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def detect_flight_segments(
    ulog, allow_simulation: bool = False
) -> list[FlightSegment]:
    """Detect and classify in-flight segments from a loaded ULog.

    Parameters
    ----------
    ulog:
        A pyulog ``ULog`` object (already loaded).
    allow_simulation:
        When ``False`` (default), a ``ValueError`` is raised if the log is a
        simulation log.  Flight-dynamics fits MUST use real logs only -- SIM
        logs carry synthetic dynamics that would poison any downstream fit.
        Set ``True`` only for unit tests on synthetic data.

    Returns
    -------
    list[FlightSegment]
        Sorted by ``start_s``.  Empty list if no flight found.

    Raises
    ------
    ValueError
        If the log is a simulation log and ``allow_simulation`` is ``False``.
    KeyError
        Only if a topic is present in the log but a required field is missing
        (indicates a firmware incompatibility worth surfacing).  Missing topics
        are handled gracefully (fallback logic or empty list).
    """
    if not allow_simulation:
        from .hardware import extract_hardware_info, is_simulation

        try:
            hw = extract_hardware_info(ulog.msg_info_dict, ulog.initial_parameters)
        except Exception:  # pragma: no cover - hardware metadata is best-effort
            hw = None
        if hw is not None and is_simulation(hw):
            raise ValueError(
                "detect_flight_segments called on a simulation log; "
                "flight-dynamics analysis requires real logs only. "
                "Pass allow_simulation=True only for synthetic test data."
            )

    tmap = topic_names(ulog)

    # -----------------------------------------------------------------------
    # Step 1: build a common time grid using vehicle_local_position if present,
    # otherwise vehicle_status timestamps.
    # -----------------------------------------------------------------------
    if "vehicle_local_position" in tmap:
        lp_t = series(ulog, "vehicle_local_position", "z")
        t_grid = lp_t.t_s
        lp_z = lp_t.values
    elif "vehicle_status" in tmap:
        lp_z_series = series(ulog, "vehicle_status", "arming_state")
        t_grid = lp_z_series.t_s
        lp_z = np.full_like(t_grid, ALTITUDE_THRESHOLD_NED_M - 1.0)  # dummy
    else:
        return []  # nothing to work with

    if len(t_grid) < 4:
        return []

    # -----------------------------------------------------------------------
    # Step 2: compute armed / in-air boolean mask on t_grid
    # -----------------------------------------------------------------------
    mask = _armed_mask_from_status(t_grid, ulog, tmap)
    if mask is None:
        mask = _armed_mask_from_altitude(t_grid, lp_z)

    # -----------------------------------------------------------------------
    # Step 3: find contiguous in-air windows
    # -----------------------------------------------------------------------
    windows = _find_contiguous_windows(mask, t_grid)
    if not windows:
        return []

    # -----------------------------------------------------------------------
    # Step 4: classify each window using local-position velocities
    # -----------------------------------------------------------------------
    # Pre-fetch velocity series (optional; fall back to UNKNOWN if absent)
    vx_series: Series | None = None
    vy_series: Series | None = None
    vz_series: Series | None = None
    ang_acc_series: np.ndarray | None = None
    ang_acc_t: np.ndarray | None = None

    if "vehicle_local_position" in tmap:
        vx_series = series(ulog, "vehicle_local_position", "vx")
        vy_series = series(ulog, "vehicle_local_position", "vy")
        vz_series = series(ulog, "vehicle_local_position", "vz")

    if "vehicle_angular_acceleration" in tmap:
        ax_s = series(ulog, "vehicle_angular_acceleration", "xyz[0]")
        ay_s = series(ulog, "vehicle_angular_acceleration", "xyz[1]")
        az_s = series(ulog, "vehicle_angular_acceleration", "xyz[2]")
        ang_acc_t = ax_s.t_s
        ang_acc_series = np.sqrt(ax_s.values**2 + ay_s.values**2 + az_s.values**2)

    segments: list[FlightSegment] = []
    for t_start, t_end in windows:
        # Window mask on t_grid
        win_mask = (t_grid >= t_start) & (t_grid <= t_end)
        n_samples = int(np.sum(win_mask))

        if vz_series is None:
            segments.append(
                FlightSegment(
                    start_s=t_start,
                    end_s=t_end,
                    kind=SegmentKind.UNKNOWN,
                    n_samples=n_samples,
                )
            )
            continue

        t_win = t_grid[win_mask]
        vz_win = _interp_at(t_win, vz_series.t_s, vz_series.values)
        vx_win = _interp_at(t_win, vx_series.t_s, vx_series.values)  # type: ignore[union-attr]
        vy_win = _interp_at(t_win, vy_series.t_s, vy_series.values)  # type: ignore[union-attr]

        if ang_acc_series is not None and ang_acc_t is not None:
            ang_win = _interp_at(t_win, ang_acc_t, ang_acc_series)
        else:
            ang_win = np.full_like(t_win, 0.0)

        kinds = [
            _classify_sample(
                float(vz_win[i]), float(vx_win[i]), float(vy_win[i]), float(ang_win[i])
            )
            for i in range(len(t_win))
        ]

        kind = _majority_kind(kinds)
        segments.append(
            FlightSegment(
                start_s=t_start,
                end_s=t_end,
                kind=kind,
                n_samples=n_samples,
            )
        )

    return sorted(segments, key=lambda s: s.start_s)


# ---------------------------------------------------------------------------
# Topic inventory
# ---------------------------------------------------------------------------

#: Topics relevant to downstream fitting plans.
TOPICS_OF_INTEREST = [
    "actuator_motors",
    "actuator_outputs",
    "esc_status",
    "battery_status",
    "vehicle_angular_acceleration",
    "vehicle_acceleration",
    "vehicle_local_position",
    "vehicle_attitude",
    "vehicle_rates_setpoint",
    "vehicle_angular_velocity",
    "wind_estimate",
    "sensor_gps",
    "vehicle_gps_position",
    "estimator_status",
    "sensor_combined",
    "vehicle_status",
    "vehicle_land_detected",
]


@dataclass(frozen=True)
class TopicStats:
    """Per-topic availability across the corpus."""

    topic: str
    real_log_count: int  # number of real (non-SIM) logs that carry this topic
    sim_log_count: int  # number of SIM logs that carry this topic
    real_median_samples: float  # median sample count across real logs that have it


@dataclass(frozen=True)
class InventoryResult:
    """Full corpus inventory result."""

    total_logs: int
    real_logs: int
    sim_logs: int
    failed_logs: int
    topics: list[TopicStats]


def topic_inventory(log_dir: str) -> InventoryResult:
    """Scan *log_dir* for .ulg files and build a topic-availability matrix.

    SIM logs are counted separately but excluded from ``real_*`` statistics.

    Parameters
    ----------
    log_dir:
        Path to a directory containing ``*.ulg`` files.

    Returns
    -------
    InventoryResult
        Availability matrix + corpus counts.
    """
    from pathlib import Path
    from pyulog import ULog
    from .hardware import HardwareInfo, extract_hardware_info, is_simulation

    paths = sorted(Path(log_dir).glob("*.ulg"))

    total = 0
    real_count = 0
    sim_count = 0
    failed = 0

    # topic → {real: [sample_counts], sim: [sample_counts]}
    real_samples: dict[str, list[int]] = {t: [] for t in TOPICS_OF_INTEREST}
    sim_samples: dict[str, list[int]] = {t: [] for t in TOPICS_OF_INTEREST}

    for path in paths:
        total += 1
        try:
            ulog = ULog(str(path))
        except Exception:
            failed += 1
            continue

        # Determine real vs sim
        hw: HardwareInfo | None = None
        try:
            hw = extract_hardware_info(ulog.msg_info_dict, ulog.initial_parameters)
        except Exception:
            pass

        is_sim = hw is not None and is_simulation(hw)

        tmap = topic_names(ulog)

        for topic in TOPICS_OF_INTEREST:
            if topic in tmap:
                n = tmap[topic]
                if is_sim:
                    sim_samples[topic].append(n)
                else:
                    real_samples[topic].append(n)

        if is_sim:
            sim_count += 1
        else:
            real_count += 1

    topic_stats: list[TopicStats] = []
    for topic in TOPICS_OF_INTEREST:
        real_ns = real_samples[topic]
        sim_ns = sim_samples[topic]
        median_real = float(np.median(real_ns)) if real_ns else 0.0
        topic_stats.append(
            TopicStats(
                topic=topic,
                real_log_count=len(real_ns),
                sim_log_count=len(sim_ns),
                real_median_samples=median_real,
            )
        )

    return InventoryResult(
        total_logs=total,
        real_logs=real_count,
        sim_logs=sim_count,
        failed_logs=failed,
        topics=topic_stats,
    )


# ---------------------------------------------------------------------------
# Feasibility notes for downstream plans
# ---------------------------------------------------------------------------


def _feasibility_note(topic: str, real_count: int, total_real: int) -> str:
    """Return a brief feasibility note for the coverage report."""
    if total_real == 0:
        return "no real logs"
    pct = 100 * real_count / total_real
    plan_notes: dict[str, str] = {
        "esc_status": "motor sysID (plan 1a)",
        "battery_status": "battery model (plan 2)",
        "wind_estimate": "disturbance/wind model (plan 5)",
        "estimator_status": "EKF / timing calibration (plan 7)",
        "actuator_motors": "open-loop replay (plan 3)",
        "vehicle_angular_acceleration": "inertia sysID (plan 1b)",
        "sensor_gps": "GPS fault models (plan 6)",
    }
    note = plan_notes.get(topic, "")
    if pct >= 80:
        feasibility = "feasible (data-rich)"
    elif pct >= 30:
        feasibility = "feasible (partial coverage)"
    else:
        feasibility = "thin — may require targeted re-flights"
    suffix = f" → {note}" if note else ""
    return f"{pct:.0f}% of real logs{suffix} — {feasibility}"


def write_coverage_report(
    result: InventoryResult, output_path: str, sample_note: str = ""
) -> None:
    """Write a Markdown coverage report to *output_path*."""
    from pathlib import Path

    lines: list[str] = [
        "# Log Topic Coverage Report",
        "",
        "Generated: 2026-06-23",
        "",
    ]

    if sample_note:
        lines += [f"> **Note:** {sample_note}", ""]

    lines += [
        "## Corpus Summary",
        "",
        "| Metric | Count |",
        "|--------|-------|",
        f"| Total logs scanned | {result.total_logs} |",
        f"| Real (non-SIM) logs | {result.real_logs} |",
        f"| Simulation logs (SIM/SITL) | {result.sim_logs} |",
        f"| Failed to load | {result.failed_logs} |",
        "",
        "## Topic Availability Matrix",
        "",
        "Topics relevant to downstream fitting plans.  "
        "*Real-log count* excludes SIM logs.  "
        "*Median samples* is the median across logs that carry the topic.",
        "",
        "| Topic | Real logs with it | Median samples | % of real | Feasibility |",
        "|-------|-------------------|----------------|-----------|-------------|",
    ]

    for ts in result.topics:
        pct = (100 * ts.real_log_count / result.real_logs) if result.real_logs else 0
        note = _feasibility_note(ts.topic, ts.real_log_count, result.real_logs)
        lines.append(
            f"| `{ts.topic}` | {ts.real_log_count} | "
            f"{ts.real_median_samples:,.0f} | {pct:.0f}% | {note} |"
        )

    lines += [
        "",
        "## Key Findings",
        "",
    ]

    for ts in result.topics:
        if ts.topic in (
            "esc_status",
            "battery_status",
            "wind_estimate",
            "estimator_status",
        ):
            pct = (
                (100 * ts.real_log_count / result.real_logs) if result.real_logs else 0
            )
            lines.append(
                f"- **`{ts.topic}`**: present in {ts.real_log_count}/{result.real_logs} "
                f"real logs ({pct:.0f}%) — "
                + _feasibility_note(ts.topic, ts.real_log_count, result.real_logs)
            )

    lines += [""]
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    Path(output_path).write_text("\n".join(lines) + "\n", encoding="utf-8")


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------


def _main() -> None:
    import argparse

    parser = argparse.ArgumentParser(
        description="Scan a directory of .ulg logs and produce a topic-coverage report."
    )
    parser.add_argument(
        "log_dir",
        nargs="?",
        default="data/downloaded",
        help="Directory containing *.ulg files (default: data/downloaded)",
    )
    parser.add_argument(
        "--output",
        default="/Users/tonypo/th3seus-project/docs/superpowers/status/2026-06-23-log-topic-coverage.md",
        help="Output Markdown file path",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limit number of logs to scan (for quick sampling; full corpus if omitted)",
    )
    args = parser.parse_args()

    from pathlib import Path
    from pyulog import ULog
    from .hardware import HardwareInfo, extract_hardware_info, is_simulation

    log_dir = Path(args.log_dir)
    paths = sorted(log_dir.glob("*.ulg"))
    sample_note = ""
    if args.limit and args.limit < len(paths):
        # Sample evenly across the sorted list for representativeness
        indices = np.linspace(0, len(paths) - 1, args.limit, dtype=int)
        paths = [paths[i] for i in indices]
        sample_note = (
            f"Sampled {args.limit} logs from {log_dir} "
            f"(evenly spaced from full corpus of {len(sorted(log_dir.glob('*.ulg')))} logs). "
            f"Re-run without --limit for full corpus."
        )

    print(f"Scanning {len(paths)} logs in {log_dir} ...")

    total = 0
    real_count = 0
    sim_count = 0
    failed = 0
    real_samples: dict[str, list[int]] = {t: [] for t in TOPICS_OF_INTEREST}
    sim_samples: dict[str, list[int]] = {t: [] for t in TOPICS_OF_INTEREST}

    for i, path in enumerate(paths):
        if (i + 1) % 50 == 0:
            print(f"  {i + 1}/{len(paths)} ...")
        total += 1
        try:
            ulog = ULog(str(path))
        except Exception as exc:
            print(f"  WARN: failed to load {path.name}: {exc}")
            failed += 1
            continue

        hw: HardwareInfo | None = None
        try:
            hw = extract_hardware_info(ulog.msg_info_dict, ulog.initial_parameters)
        except Exception:
            pass

        is_sim = hw is not None and is_simulation(hw)
        tmap = topic_names(ulog)

        for topic in TOPICS_OF_INTEREST:
            if topic in tmap:
                n = tmap[topic]
                if is_sim:
                    sim_samples[topic].append(n)
                else:
                    real_samples[topic].append(n)

        if is_sim:
            sim_count += 1
        else:
            real_count += 1

    topic_stats: list[TopicStats] = []
    for topic in TOPICS_OF_INTEREST:
        real_ns = real_samples[topic]
        sim_ns = sim_samples[topic]
        median_real = float(np.median(real_ns)) if real_ns else 0.0
        topic_stats.append(
            TopicStats(
                topic=topic,
                real_log_count=len(real_ns),
                sim_log_count=len(sim_ns),
                real_median_samples=median_real,
            )
        )

    result = InventoryResult(
        total_logs=total,
        real_logs=real_count,
        sim_logs=sim_count,
        failed_logs=failed,
        topics=topic_stats,
    )

    write_coverage_report(result, args.output, sample_note)
    print(f"\nReport written to {args.output}")
    print(f"  Total: {total}  Real: {real_count}  SIM: {sim_count}  Failed: {failed}")


if __name__ == "__main__":
    _main()
