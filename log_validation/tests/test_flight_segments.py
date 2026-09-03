"""Tests for flight_segments.py using synthetic signals.

No real logs required.  We build minimal pyulog-shaped objects (using a
lightweight stub class) and inject known signals, then assert correct segment
detection and classification.
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pytest

from log_validation.flight_segments import (
    ANGULAR_ACCEL_AGGRESSIVE_RADS2,
    ARMING_STATE_ARMED,
    HORIZ_SPEED_TRANSLATION_MS,
    VZ_CLIMB_THRESHOLD_MS,
    VZ_DESCENT_THRESHOLD_MS,
    FlightSegment,
    SegmentKind,
    _classify_sample,
    _find_contiguous_windows,
    _majority_kind,
    detect_flight_segments,
)


# ---------------------------------------------------------------------------
# Minimal ULog stub
# ---------------------------------------------------------------------------

US_TO_S = 1e-6


@dataclass
class _FakeDataset:
    name: str
    data: dict[str, np.ndarray]

    def __post_init__(self) -> None:
        # pyulog stores timestamps in microseconds
        if "timestamp" not in self.data:
            raise ValueError("FakeDataset must include 'timestamp'")


class _FakeULog:
    """Minimal ULog stub that satisfies ulog_loader's API."""

    def __init__(self, datasets: list[_FakeDataset]) -> None:
        self._datasets = {d.name: d for d in datasets}
        self.data_list = datasets

    def get_dataset(self, topic: str, multi_instance: int = 0) -> _FakeDataset:
        if topic not in self._datasets:
            available = sorted(self._datasets)
            raise Exception(f"topic '{topic}' not found. Available: {available}")
        return self._datasets[topic]


def _ts(t_s: np.ndarray) -> np.ndarray:
    """Convert seconds to microseconds (integer) for fake timestamps."""
    return (t_s * 1e6).astype(np.float64)


def _make_ulog(
    *,
    duration_s: float = 60.0,
    hz: float = 100.0,
    arming_state: np.ndarray | None = None,
    landed: np.ndarray | None = None,
    vz: np.ndarray | None = None,
    vx: np.ndarray | None = None,
    vy: np.ndarray | None = None,
    ang_acc_xyz: np.ndarray | None = None,
    include_local_position: bool = True,
) -> _FakeULog:
    """Build a synthetic ULog with configurable signals.

    All arrays are sampled at *hz* over [0, duration_s].
    """
    t = np.linspace(0.0, duration_s, int(duration_s * hz) + 1)
    n = len(t)
    ts_us = _ts(t)

    datasets: list[_FakeDataset] = []

    # vehicle_status
    arm = (
        arming_state
        if arming_state is not None
        else np.full(n, ARMING_STATE_ARMED, dtype=float)
    )
    datasets.append(
        _FakeDataset(
            name="vehicle_status",
            data={"timestamp": ts_us, "arming_state": arm},
        )
    )

    # vehicle_land_detected
    if landed is not None:
        datasets.append(
            _FakeDataset(
                name="vehicle_land_detected",
                data={"timestamp": ts_us, "landed": landed},
            )
        )

    # vehicle_local_position
    if include_local_position:
        _vz = vz if vz is not None else np.zeros(n)
        _vx = vx if vx is not None else np.zeros(n)
        _vy = vy if vy is not None else np.zeros(n)
        # z = integral of -vz (NED) — negative altitude, starts at 0 then goes negative
        z = np.cumsum(-_vz) / hz
        datasets.append(
            _FakeDataset(
                name="vehicle_local_position",
                data={
                    "timestamp": ts_us,
                    "z": z,
                    "vx": _vx,
                    "vy": _vy,
                    "vz": _vz,
                },
            )
        )

    # vehicle_angular_acceleration
    if ang_acc_xyz is not None:
        datasets.append(
            _FakeDataset(
                name="vehicle_angular_acceleration",
                data={
                    "timestamp": ts_us,
                    "xyz[0]": ang_acc_xyz[:, 0],
                    "xyz[1]": ang_acc_xyz[:, 1],
                    "xyz[2]": ang_acc_xyz[:, 2],
                },
            )
        )

    return _FakeULog(datasets)


# ---------------------------------------------------------------------------
# Unit tests — _classify_sample
# ---------------------------------------------------------------------------


class TestClassifySample:
    def test_hover(self):
        assert _classify_sample(0.0, 0.0, 0.0, 0.0) == SegmentKind.HOVER

    def test_climb(self):
        assert (
            _classify_sample(VZ_CLIMB_THRESHOLD_MS - 0.1, 0.0, 0.0, 0.0)
            == SegmentKind.CLIMB
        )

    def test_descent(self):
        assert (
            _classify_sample(VZ_DESCENT_THRESHOLD_MS + 0.1, 0.0, 0.0, 0.0)
            == SegmentKind.DESCENT
        )

    def test_translation(self):
        assert (
            _classify_sample(0.0, HORIZ_SPEED_TRANSLATION_MS + 0.1, 0.0, 0.0)
            == SegmentKind.TRANSLATION
        )

    def test_aggressive_overrides_others(self):
        # Even during a climb, aggressive angular accel takes priority
        assert (
            _classify_sample(
                VZ_CLIMB_THRESHOLD_MS - 0.1,
                0.0,
                0.0,
                ANGULAR_ACCEL_AGGRESSIVE_RADS2 + 1.0,
            )
            == SegmentKind.AGGRESSIVE
        )

    def test_at_climb_threshold_boundary(self):
        # Exactly at threshold → NOT climb (strict <)
        assert (
            _classify_sample(VZ_CLIMB_THRESHOLD_MS, 0.0, 0.0, 0.0) == SegmentKind.HOVER
        )

    def test_at_descent_threshold_boundary(self):
        # Exactly at descent threshold → NOT descent (strict >)
        assert (
            _classify_sample(VZ_DESCENT_THRESHOLD_MS, 0.0, 0.0, 0.0)
            == SegmentKind.HOVER
        )

    def test_no_angular_accel(self):
        # None should be handled (no aggressive check)
        assert _classify_sample(0.0, 0.0, 0.0, None) == SegmentKind.HOVER


# ---------------------------------------------------------------------------
# Unit tests — _find_contiguous_windows
# ---------------------------------------------------------------------------


class TestFindContiguousWindows:
    def test_single_window(self):
        t = np.linspace(0.0, 10.0, 1000)
        mask = (t >= 2.0) & (t <= 8.0)
        windows = _find_contiguous_windows(mask, t)
        assert len(windows) == 1
        t_start, t_end = windows[0]
        assert abs(t_start - 2.0) < 0.02
        assert abs(t_end - 8.0) < 0.02

    def test_two_windows(self):
        t = np.linspace(0.0, 20.0, 2000)
        mask = ((t >= 1.0) & (t <= 5.0)) | ((t >= 10.0) & (t <= 15.0))
        windows = _find_contiguous_windows(mask, t)
        assert len(windows) == 2

    def test_short_window_excluded(self):
        t = np.linspace(0.0, 10.0, 1000)
        # Window of 1 second — below MIN_FLIGHT_DURATION_S
        mask = (t >= 4.0) & (t <= 4.5)
        windows = _find_contiguous_windows(mask, t)
        assert len(windows) == 0

    def test_empty_mask(self):
        t = np.linspace(0.0, 10.0, 100)
        mask = np.zeros(len(t), dtype=bool)
        assert _find_contiguous_windows(mask, t) == []

    def test_full_mask(self):
        t = np.linspace(0.0, 10.0, 1000)
        mask = np.ones(len(t), dtype=bool)
        windows = _find_contiguous_windows(mask, t)
        assert len(windows) == 1


# ---------------------------------------------------------------------------
# Unit tests — _majority_kind
# ---------------------------------------------------------------------------


class TestMajorityKind:
    def test_single(self):
        assert _majority_kind([SegmentKind.HOVER]) == SegmentKind.HOVER

    def test_majority_wins(self):
        kinds = [SegmentKind.HOVER] * 6 + [SegmentKind.CLIMB] * 4
        assert _majority_kind(kinds) == SegmentKind.HOVER

    def test_empty(self):
        assert _majority_kind([]) == SegmentKind.UNKNOWN


# ---------------------------------------------------------------------------
# Integration tests — detect_flight_segments
# ---------------------------------------------------------------------------


class TestDetectFlightSegments:
    def test_no_flight_returns_empty(self):
        """All samples disarmed → empty result."""
        # _make_ulog with duration_s=10, hz=100 → 1001 samples
        duration_s = 10.0
        hz = 100.0
        n = int(duration_s * hz) + 1
        arming_state = np.ones(n)  # 1 = standby, not armed
        ulog = _make_ulog(
            duration_s=duration_s,
            hz=hz,
            arming_state=arming_state,
            include_local_position=False,
        )
        assert detect_flight_segments(ulog) == []

    def test_simple_hover_flight(self):
        """Armed the whole time, no velocity → one HOVER segment."""
        segments = detect_flight_segments(_make_ulog(duration_s=30.0))
        assert len(segments) >= 1
        assert all(s.kind == SegmentKind.HOVER for s in segments)

    def test_climb_classification(self):
        """Constant upward velocity (vz strongly negative in NED) → CLIMB."""
        n = 3001
        vz = np.full(n, VZ_CLIMB_THRESHOLD_MS - 1.0)  # strong climb
        ulog = _make_ulog(duration_s=30.0, vz=vz)
        segments = detect_flight_segments(ulog)
        assert len(segments) >= 1
        assert segments[0].kind == SegmentKind.CLIMB

    def test_descent_classification(self):
        """Constant downward velocity → DESCENT."""
        n = 3001
        vz = np.full(n, VZ_DESCENT_THRESHOLD_MS + 1.0)  # strong descent
        ulog = _make_ulog(duration_s=30.0, vz=vz)
        segments = detect_flight_segments(ulog)
        assert len(segments) >= 1
        assert segments[0].kind == SegmentKind.DESCENT

    def test_translation_classification(self):
        """High horizontal speed → TRANSLATION."""
        n = 3001
        vx = np.full(n, HORIZ_SPEED_TRANSLATION_MS + 2.0)
        ulog = _make_ulog(duration_s=30.0, vx=vx)
        segments = detect_flight_segments(ulog)
        assert len(segments) >= 1
        assert segments[0].kind == SegmentKind.TRANSLATION

    def test_aggressive_classification(self):
        """Large angular acceleration → AGGRESSIVE."""
        n = 3001
        ang = np.zeros((n, 3))
        ang[:, 0] = ANGULAR_ACCEL_AGGRESSIVE_RADS2 + 5.0  # x-axis only
        ulog = _make_ulog(duration_s=30.0, ang_acc_xyz=ang)
        segments = detect_flight_segments(ulog)
        assert len(segments) >= 1
        assert segments[0].kind == SegmentKind.AGGRESSIVE

    def test_landed_flag_gates_segment(self):
        """Armed but landed=1 → no in-air segment."""
        n = 3001
        arming = np.full(n, ARMING_STATE_ARMED, dtype=float)
        landed = np.ones(n)  # always landed
        ulog = _make_ulog(duration_s=30.0, arming_state=arming, landed=landed)
        assert detect_flight_segments(ulog) == []

    def test_partial_flight_window(self):
        """Armed for first half only → one segment, not the full duration."""
        n = 6001
        arming = np.where(np.linspace(0.0, 60.0, n) < 30.0, ARMING_STATE_ARMED, 1.0)
        ulog = _make_ulog(duration_s=60.0, arming_state=arming)
        segments = detect_flight_segments(ulog)
        assert len(segments) == 1
        assert segments[0].end_s < 35.0  # ends in first half

    def test_two_separate_flights(self):
        """Disarm in the middle → two separate segments."""
        n = 12001
        t = np.linspace(0.0, 120.0, n)
        arming = np.where(
            ((t >= 5.0) & (t <= 50.0)) | ((t >= 70.0) & (t <= 115.0)),
            ARMING_STATE_ARMED,
            1.0,
        )
        ulog = _make_ulog(duration_s=120.0, arming_state=arming)
        segments = detect_flight_segments(ulog)
        assert len(segments) == 2
        assert segments[0].start_s < 10.0
        assert segments[1].start_s > 60.0

    def test_missing_vehicle_status_no_crash(self):
        """Log without vehicle_status falls back gracefully (returns [] or altitude-based)."""
        t = np.linspace(0.0, 30.0, 3001)
        ts_us = _ts(t)
        n = len(t)
        ds_lp = _FakeDataset(
            name="vehicle_local_position",
            data={
                "timestamp": ts_us,
                "z": np.full(n, -5.0),  # 5 m above ground in NED
                "vx": np.zeros(n),
                "vy": np.zeros(n),
                "vz": np.zeros(n),
            },
        )
        ulog = _FakeULog([ds_lp])
        # Should not raise; returns segments (altitude fallback)
        segments = detect_flight_segments(ulog)
        assert isinstance(segments, list)

    def test_segment_is_frozen(self):
        """FlightSegment must be immutable (frozen dataclass)."""
        seg = FlightSegment(
            start_s=0.0, end_s=10.0, kind=SegmentKind.HOVER, n_samples=100
        )
        with pytest.raises((AttributeError, TypeError)):
            seg.start_s = 5.0  # type: ignore[misc]

    def test_n_samples_positive(self):
        """Each returned segment must have at least 1 sample."""
        segments = detect_flight_segments(_make_ulog(duration_s=30.0))
        for seg in segments:
            assert seg.n_samples > 0

    def test_segments_sorted(self):
        """Returned segments must be sorted by start_s."""
        n = 12001
        t = np.linspace(0.0, 120.0, n)
        arming = np.where(
            ((t >= 5.0) & (t <= 50.0)) | ((t >= 70.0) & (t <= 115.0)),
            ARMING_STATE_ARMED,
            1.0,
        )
        ulog = _make_ulog(duration_s=120.0, arming_state=arming)
        segments = detect_flight_segments(ulog)
        starts = [s.start_s for s in segments]
        assert starts == sorted(starts)
