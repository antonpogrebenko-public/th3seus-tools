"""Tests for grouped noise profiles."""

from __future__ import annotations

import dataclasses

import numpy as np
import pytest

from log_validation.aggregate import (
    ProfileGroup,
    SensorStats,
    aggregate_profiles,
    mcu_family,
    to_json,
)

GYRO = "gyro.gyro_noise_density"


def _entry(value, mcu="STM32F7xx", board="fmu-v5", imu="ICM20689"):
    return {
        "log_id": f"log-{value}",
        "hardware": {"fc_board": board, "imu_name": imu, "mcu": mcu},
        "measurements": {GYRO: value},
    }


def _by_key(groups):
    return {(g.kind, g.key): g for g in groups}


@pytest.mark.parametrize(
    "mcu, expected",
    [
        ("STM32F4xx", "STM32F4"),
        ("stm32f7xx", "STM32F7"),
        ("STM32H743", "STM32H7"),
        ("Kinetis K66", "NXP_K66"),
        ("MK66FN2M0", "NXP_K66"),
        ("ESP32", None),
        ("", None),
        (None, None),
    ],
)
def test_mcu_family_mapping(mcu, expected):
    assert mcu_family(mcu) == expected


def test_mcu_family_profile():
    results = [_entry(0.001 * (i + 1), mcu="STM32F7xx", board=f"b{i}", imu=f"i{i}") for i in range(10)]
    results += [_entry(0.002 * (i + 1), mcu="STM32H743", board=f"h{i}", imu=f"j{i}") for i in range(2)]
    groups = aggregate_profiles(results)
    keys = {(g.kind, g.key): g.sample_count for g in groups}
    assert keys[("mcu_family", "STM32F7")] == 10
    assert ("mcu_family", "STM32H7") not in keys
    assert keys[("mcu_family", "ALL")] == 12


def test_percentiles_after_outlier_filter():
    values = [float(v) for v in range(1, 21)] + [1000.0]
    results = [_entry(v) for v in values]
    group = _by_key(aggregate_profiles(results))[("mcu_family", "STM32F7")]
    stats = group.sensors[GYRO]
    base = np.arange(1, 21, dtype=float)
    assert stats.count == 20
    assert stats.p5 == pytest.approx(np.percentile(base, 5))
    assert stats.p25 == pytest.approx(np.percentile(base, 25))
    assert stats.median == pytest.approx(np.median(base))
    assert stats.p75 == pytest.approx(np.percentile(base, 75))
    assert stats.p95 == pytest.approx(np.percentile(base, 95))
    assert group.sample_count == 21


def test_board_and_imu_groups():
    results = [_entry(0.001 * (i + 1)) for i in range(3)]
    keys = {(g.kind, g.key): g.sample_count for g in aggregate_profiles(results)}
    assert keys[("board", "fmu-v5")] == 3
    assert keys[("imu", "ICM20689")] == 3


def test_min_logs_and_missing_fields():
    results = [_entry(0.001 * (i + 1), board=None, imu=None) for i in range(3)]
    keys = {(g.kind, g.key) for g in aggregate_profiles(results)}
    assert not any(k[0] in ("board", "imu") for k in keys)
    assert aggregate_profiles(results[:2]) == []
    assert ("mcu_family", "ALL") in {(g.kind, g.key) for g in aggregate_profiles(results[:2], min_logs=2)}


def test_sorted_by_kind_then_count_then_key():
    results = [_entry(0.001 * (i + 1), board="a", imu="x") for i in range(3)]
    results += [_entry(0.01 * (i + 1), board="b", imu="y") for i in range(4)]
    order = [(g.kind, g.key, g.sample_count) for g in aggregate_profiles(results)]
    assert order == sorted(order, key=lambda t: (t[0], -t[2], t[1]))
    assert order[0][0] == "board"


def test_inputs_not_mutated_and_frozen():
    results = [_entry(0.001 * (i + 1)) for i in range(3)]
    snapshot = repr(results)
    groups = aggregate_profiles(results)
    assert repr(results) == snapshot
    with pytest.raises(dataclasses.FrozenInstanceError):
        groups[0].key = "x"
    assert isinstance(groups[0], ProfileGroup)
    assert isinstance(next(iter(groups[0].sensors.values())), SensorStats)


def test_to_json_includes_kind_key_and_percentiles():
    out = to_json(aggregate_profiles([_entry(0.001 * (i + 1)) for i in range(3)]))
    row = out[0]
    assert {"kind", "key", "sample_count", "sensors"} <= set(row)
    sensor = row["sensors"][GYRO]
    assert "p5" in sensor and "p95" in sensor
