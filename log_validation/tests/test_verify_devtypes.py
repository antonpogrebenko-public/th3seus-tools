"""Pure-function tests for the firmware-aware devtype audit (no network)."""

from __future__ import annotations

from log_validation.verify_devtypes import _same_part, primary_devtype


def test_same_part_accepts_upstream_typo():
    # PX4 spells the FXAS21002 gyro "FXAS2100C" in older source; same chip.
    assert _same_part("FXAS2100C", "FXAS21002C")


def test_same_part_rejects_different_chip():
    # Distinct ICM parts must not be collapsed into one.
    assert not _same_part("ICM20602", "ICM20689")


def test_primary_devtype_prefers_gyro_high_byte():
    hw = {"gyro_ids": [0x3C000A], "accel_ids": [0x21000B]}
    assert primary_devtype(hw) == 0x3C


def test_primary_devtype_falls_back_to_accel():
    hw = {"gyro_ids": [], "accel_ids": [0x21000B]}
    assert primary_devtype(hw) == 0x21


def test_primary_devtype_none_without_ids():
    assert primary_devtype({}) is None
