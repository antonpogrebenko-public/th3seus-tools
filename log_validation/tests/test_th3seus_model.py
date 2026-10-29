"""Tests for the hitl-physics 0.13.1 mirror: reference builds, PX4's unit
torque allocation and the control effectiveness per axis."""

from __future__ import annotations

import dataclasses
import math

import numpy as np
import pytest

from log_validation.th3seus_model import (
    HITL_PHYSICS_VERSION,
    PROP_COEFFICIENT_TABLE,
    REFERENCE_BUILDS,
    Build,
    predict,
    prop_coefficients,
    prop_coefficients_from_basis,
    unit_torque_motor_deltas,
)

# Printed by hitl-physics 0.13.1's own code: BuildSpec::to_physics_config()
# on each reference build, and QuadrotorState::compute_torques() differenced
# about the hover command for the effectiveness (program in the task report).
HITL_PHYSICS_PRINTED: dict[str, dict[str, float]] = {
    "tinyhawk_3": {
        "mass_kg": 0.0445,
        "ixx": 2.5640461900134933e-5,
        "iyy": 2.5640461900134933e-5,
        "izz": 4.488254599653493e-5,
        "tau_motor_s": 0.021,
        "kt": 1.4208892945678067e-8,
        "kq": 1.3831723441675354e-10,
        "omega_max_rad_s": 4551.013983697108,
        "hover_command": 0.6088664675862393,
        "thrust_to_weight": 2.697465425212165,
        "effectiveness_roll": 1226.193020069881,
        "effectiveness_pitch": 1226.193020069881,
        "effectiveness_yaw": 310.9046343531602,
    },
    "impulse_apex_5": {
        "mass_kg": 0.542,
        "ixx": 0.003260904445920001,
        "iyy": 0.003260904445920001,
        "izz": 0.005837028325920002,
        "tau_motor_s": 0.028823529411764706,
        "kt": 1.4806698057109176e-6,
        "kq": 1.7347158288057744e-8,
        "omega_max_rad_s": 2424.6620291812173,
        "hover_command": 0.39070562510526846,
        "thrust_to_weight": 6.550895507728478,
        "effectiveness_roll": 582.812196150018,
        "effectiveness_pitch": 582.812196150018,
        "effectiveness_yaw": 54.61080949247067,
    },
    "geprc_mark5": {
        "mass_kg": 0.611,
        "ixx": 0.003060005815460001,
        "iyy": 0.003060005815460001,
        "izz": 0.005362904435460001,
        "tau_motor_s": 0.027653061224489794,
        "kt": 1.452849287440775e-6,
        "kq": 1.660606735544806e-8,
        "omega_max_rad_s": 3485.1737020502696,
        "hover_command": 0.2913508145050839,
        "thrust_to_weight": 11.78060316701764,
        "effectiveness_roll": 938.9010041548019,
        "effectiveness_pitch": 938.9010041548019,
        "effectiveness_yaw": 87.66410781821828,
    },
    "iflight_chimera_7": {
        "mass_kg": 1.545,
        "ixx": 0.013497557158057501,
        "iyy": 0.013497557158057501,
        "izz": 0.024159197125057503,
        "tau_motor_s": 0.032,
        "kt": 7.2142081439263295e-6,
        "kq": 8.039692374123753e-8,
        "omega_max_rad_s": 1802.7023371906964,
        "hover_command": 0.40195388260363046,
        "thrust_to_weight": 6.189385654259384,
        "effectiveness_roll": 585.2014873310598,
        "effectiveness_pitch": 585.2014873310598,
        "effectiveness_yaw": 34.77532461453647,
    },
}

# The same program on two propellers that are not rows of hitl-physics' table,
# so its lookup rescales the nearest row. Both are fitted to impulse_apex_5.
# The second ties between the 8x5x2 and 10x5x2 rows; the first row wins.
HITL_PHYSICS_PRINTED_OFF_TABLE: dict[tuple[float, float, int], dict[str, float]] = {
    (5.1, 4.6, 3): {
        "kt": 1.6328384397259183e-6,
        "kq": 1.9829803450465422e-8,
        "omega_max_rad_s": 2399.5450598698994,
        "hover_command": 0.3759494620512596,
    },
    (9.0, 5.0, 2): {
        "kt": 1.2215713524044061e-5,
        "kq": 1.5707883017561433e-7,
        "omega_max_rad_s": 924.0045107166555,
        "hover_command": 0.35694103154268064,
    },
}

PINNED_FIELDS = (
    "mass_kg",
    "ixx",
    "iyy",
    "izz",
    "tau_motor_s",
    "kt",
    "kq",
    "omega_max_rad_s",
    "hover_command",
    "thrust_to_weight",
)
EFFECTIVENESS_FIELDS = ("effectiveness_roll", "effectiveness_pitch", "effectiveness_yaw")
INV_SQRT2 = 1.0 / math.sqrt(2.0)


def test_mirrors_hitl_physics_0_13_1():
    assert HITL_PHYSICS_VERSION == "0.13.1"


def test_reference_build_thrust_to_weight():
    # hitl-physics CHANGELOG.md:50
    expected = {"tinyhawk_3": 2.70, "impulse_apex_5": 6.55, "geprc_mark5": 11.78, "iflight_chimera_7": 6.19}
    for name, twr in expected.items():
        assert predict(REFERENCE_BUILDS[name]).thrust_to_weight == pytest.approx(twr, rel=0.01)


def test_apex_hover_and_lag():
    p = predict(REFERENCE_BUILDS["impulse_apex_5"])
    assert p.hover_command == pytest.approx(0.391, abs=0.001)
    assert p.tau_motor_s == pytest.approx(0.0288, abs=0.0001)


def test_reference_build_inertia():
    assert set(HITL_PHYSICS_PRINTED) == set(REFERENCE_BUILDS)
    for name, printed in HITL_PHYSICS_PRINTED.items():
        p = predict(REFERENCE_BUILDS[name])
        assert p.ixx == pytest.approx(printed["ixx"], rel=1e-12), name
        assert p.izz == pytest.approx(printed["izz"], rel=1e-12), name


@pytest.mark.parametrize("field", PINNED_FIELDS)
@pytest.mark.parametrize("name", ["tinyhawk_3", "impulse_apex_5", "geprc_mark5", "iflight_chimera_7"])
def test_reference_build_matches_hitl_physics(name, field):
    printed = HITL_PHYSICS_PRINTED[name][field]
    assert getattr(predict(REFERENCE_BUILDS[name]), field) == pytest.approx(printed, rel=1e-12)


@pytest.mark.parametrize("field", EFFECTIVENESS_FIELDS)
@pytest.mark.parametrize("name", ["tinyhawk_3", "impulse_apex_5", "geprc_mark5", "iflight_chimera_7"])
def test_reference_build_effectiveness_matches_hitl_physics_torques(name, field):
    printed = HITL_PHYSICS_PRINTED[name][field]
    assert getattr(predict(REFERENCE_BUILDS[name]), field) == pytest.approx(printed, rel=1e-9)


@pytest.mark.parametrize("prop", [(5.1, 4.6, 3), (9.0, 5.0, 2)])
def test_off_table_propeller_matches_hitl_physics_lookup(prop):
    diameter, pitch, blades = prop
    build = dataclasses.replace(
        REFERENCE_BUILDS["impulse_apex_5"],
        prop_diameter_in=diameter,
        prop_pitch_in=pitch,
        prop_blade_count=blades,
    )
    p = predict(build)
    printed = HITL_PHYSICS_PRINTED_OFF_TABLE[prop]
    for field in ("kt", "kq", "omega_max_rad_s", "hover_command"):
        assert getattr(p, field) == pytest.approx(printed[field], rel=1e-12), field


def test_prop_table_caches_the_closed_form_basis():
    # hitl-physics data/prop_coefficients.csv: 52 rows, each the basis to 7 figures
    assert len(PROP_COEFFICIENT_TABLE) == 52
    for diameter, pitch, blades, ct, cq in PROP_COEFFICIENT_TABLE:
        basis_ct, basis_cq = prop_coefficients_from_basis(diameter, pitch, blades)
        assert ct == pytest.approx(basis_ct, rel=5e-7)
        assert cq == pytest.approx(basis_cq, rel=5e-7)


def test_prop_lookup_takes_an_exact_row_as_it_is():
    assert prop_coefficients(5.0, 4.3, 3) == (4.646300e-3, 4.286212e-4)


@pytest.mark.parametrize("lookup", [prop_coefficients, prop_coefficients_from_basis])
def test_prop_coefficients_reject_an_impossible_propeller(lookup):
    with pytest.raises(ValueError, match="diameter_in"):
        lookup(0.0, 4.0, 3)
    with pytest.raises(ValueError, match="blade_count"):
        lookup(5.0, 4.0, 0)


def test_unit_roll_allocation_is_antisymmetric():
    d = unit_torque_motor_deltas("roll")
    assert d[0] == pytest.approx(-d[2]) and d[1] == pytest.approx(-d[3])


@pytest.mark.parametrize(
    ("axis", "expected"),
    [
        # Motor order 1 FR, 2 BL, 3 FL, 4 BR.
        ("roll", [-INV_SQRT2, INV_SQRT2, INV_SQRT2, -INV_SQRT2]),
        ("pitch", [INV_SQRT2, -INV_SQRT2, INV_SQRT2, -INV_SQRT2]),
        ("yaw", [1.0, 1.0, -1.0, -1.0]),
    ],
)
def test_unit_torque_deltas_follow_px4_normalization(axis, expected):
    np.testing.assert_allclose(unit_torque_motor_deltas(axis), expected, rtol=1e-6, atol=1e-6)


def test_unit_torque_deltas_reject_an_unknown_axis():
    with pytest.raises(ValueError, match="thrust"):
        unit_torque_motor_deltas("thrust")


def test_unit_torque_deltas_return_a_fresh_array():
    first = unit_torque_motor_deltas("yaw")
    first[:] = 0.0
    np.testing.assert_allclose(unit_torque_motor_deltas("yaw"), [1.0, 1.0, -1.0, -1.0], atol=1e-6)


def test_effectiveness_is_positive_and_equal_on_roll_and_pitch():
    for name, build in REFERENCE_BUILDS.items():
        p = predict(build)
        assert p.effectiveness_roll > 0 and p.effectiveness_yaw > 0, name
        assert p.effectiveness_pitch == pytest.approx(p.effectiveness_roll, rel=1e-12), name


def test_hover_command_is_one_over_root_thrust_to_weight():
    for build in REFERENCE_BUILDS.values():
        p = predict(build)
        assert p.hover_command == pytest.approx(1.0 / math.sqrt(p.thrust_to_weight), rel=1e-12)


def test_absent_weights_take_hitl_physics_defaults():
    base = REFERENCE_BUILDS["geprc_mark5"]
    absent = dataclasses.replace(base, prop_weight_g=None, esc_total_weight_g=None, gps_weight_g=None)
    explicit = dataclasses.replace(base, prop_weight_g=3.0, esc_total_weight_g=12.0, gps_weight_g=0.0)
    assert predict(absent).mass_kg == pytest.approx(predict(explicit).mass_kg, rel=1e-15)
    assert predict(absent).mass_kg != pytest.approx(predict(base).mass_kg)


def test_absent_motor_and_esc_values_take_hitl_physics_defaults():
    base = REFERENCE_BUILDS["iflight_chimera_7"]
    absent = dataclasses.replace(
        base, motor_resistance_ohm=None, motor_no_load_amps=None, esc_burst_amps=None
    )
    explicit = dataclasses.replace(
        base,
        motor_resistance_ohm=0.05 + 0.02 / max(base.motor_kv / 1000.0, 0.5),
        motor_no_load_amps=0.5,
        esc_burst_amps=base.esc_continuous_amps * 1.2,
    )
    assert predict(absent) == predict(explicit)


def test_absent_battery_voltage_is_cells_times_3_7():
    base = REFERENCE_BUILDS["impulse_apex_5"]
    explicit = dataclasses.replace(base, battery_voltage_v=base.battery_cells * 3.7)
    assert predict(base) == predict(explicit)
    sagged = dataclasses.replace(base, battery_voltage_v=13.0)
    assert predict(sagged).omega_max_rad_s < predict(base).omega_max_rad_s


def test_build_and_prediction_are_frozen_and_predict_is_pure():
    build = REFERENCE_BUILDS["tinyhawk_3"]
    before = dataclasses.asdict(build)
    p = predict(build)
    assert dataclasses.asdict(build) == before
    assert predict(build) == p
    with pytest.raises(dataclasses.FrozenInstanceError):
        build.motor_kv = 1.0  # type: ignore[misc]
    with pytest.raises(dataclasses.FrozenInstanceError):
        p.kt = 1.0  # type: ignore[misc]


@pytest.mark.parametrize(
    "change",
    [
        {"motor_kv": 0.0},
        {"prop_diameter_in": -5.0},
        {"prop_blade_count": 0},
        {"battery_cells": 0},
        {"motor_resistance_ohm": 0.0},
        {"battery_voltage_v": float("nan")},
    ],
)
def test_build_rejects_values_hitl_physics_cannot_use(change):
    fields = dataclasses.asdict(REFERENCE_BUILDS["impulse_apex_5"]) | change
    with pytest.raises(ValueError, match=next(iter(change))):
        Build(**fields)


def test_predict_rejects_a_build_without_usable_motor_current():
    build = dataclasses.replace(REFERENCE_BUILDS["impulse_apex_5"], motor_no_load_amps=60.0)
    with pytest.raises(ValueError, match="current"):
        predict(build)
