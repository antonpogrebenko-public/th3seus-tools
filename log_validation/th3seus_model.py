"""TH3SEUS physics predictions for a quadrotor build, mirrored from hitl-physics.

The single source of truth is the Rust crate hitl-physics. This module mirrors
version 0.13.1 (`HITL_PHYSICS_VERSION`) of the path the TH3SEUS daemon takes
from a build to its physics, `BuildSpec::to_physics_config`
(hitl-physics src/build.rs:500-551). Every constant cites the hitl-physics file
and line it is copied from; the tests pin the mirror to values printed by
hitl-physics 0.13.1 itself for its four reference builds, so a release that
changes a formula fails them once the pinned values are updated.

Mirrored quantities:

* mass: the sum of all components, with 3.0 g per propeller and 12.0 g for all
  ESCs when absent and no GPS when absent (src/build.rs:635-647);
* inertia: the legacy point-mass estimate from the frame and motor weights,
  arm = 1.1 x propeller diameter (src/config.rs:201-215);
* kt and kq: propeller coefficients resolved as `prop_coefficients::lookup`
  does, the exact row of data/prop_coefficients.csv or else the nearest row
  rescaled by pitch and blade count (src/prop_coefficients.rs:129-190 and
  213-279). Each row of that table is the closed-form basis evaluated at the
  row's geometry, see `prop_coefficients_from_basis`;
* full-throttle motor speed: min(voltage limit, current limit), the current
  limit being the ESC burst rating or the pack's C-rating shared by four
  motors (src/config.rs:283-313, src/build.rs:532-548);
* motor lag: tau = 0.02 + (1500 / KV) * 0.01 s (src/config.rs:199). The ESC
  response time is a data carrier that the loop does not use (src/config.rs:3-9);
* hover command: hover speed over full-throttle speed (src/config.rs:266-270
  and 389-391), and thrust-to-weight 4 * kt * omega_max^2 / (m * g).

Motor command. TH3SEUS maps a motor command u in [0, 1] linearly to a commanded
speed omega = u * omega_max (src/motor.rs:22 and 34-46); thrust is kt * omega^2
and reaction torque kq * omega^2 (src/motor.rs:89-106). Thrust is therefore
kt * omega_max^2 * u^2, and at the hover command its slope is
df/du = 2 * kt * omega_max^2 * u_hover (2 * kq * omega_max^2 * u_hover for the
reaction torque).

Control effectiveness is angular acceleration in rad/s^2 per unit of PX4's
normalized `vehicle_torque_setpoint`, after PX4's control allocation, which is
the unit the response pass measures in logs. A unit setpoint on one axis moves
the four motor commands by `unit_torque_motor_deltas(axis)`. The slopes above
turn those changes into thrust and reaction-torque changes, the quad-X geometry
of src/quadrotor.rs:92-127 into body torque (lever arm * cos 45 deg on roll
and pitch, reaction torques on yaw), and the inertia about the axis into
angular acceleration.

PX4 allocation, cited at PX4-Autopilot tag v1.14.0 (the file is identical at
v1.15.0): src/modules/control_allocator/ControlAllocation/
ControlAllocationPseudoInverse.cpp, `updateControlAllocationMatrixScale` and
`normalizeControlAllocationMatrix`. The allocation matrix is the
pseudo-inverse of the rotor effectiveness matrix
(ActuatorEffectiveness/ActuatorEffectivenessRotors.cpp: rotor thrust CT * axis,
moment CT * position x axis - CT * KM * axis). A multicopter normalizes its
roll, pitch and yaw columns (ActuatorEffectivenessMultirotor.hpp,
`getNormalizeRPY`):

* roll and pitch share one scale, the larger over the two axes of
  sqrt(squared column norm / (number of non-zero entries / 2));
* yaw is scaled by the largest entry of its column.

On a symmetric quad X (|PX| = |PY| for every rotor, as in PX4's airframe
1001_rc_quad_x.hil) a unit roll or pitch setpoint therefore moves each motor
command by 1/sqrt(2) and a unit yaw setpoint by 1, whatever the arm, CT and KM.
With CA_METHOD at its default (automatic) a multicopter allocates by sequential
desaturation (ActuatorEffectivenessMultirotor.hpp, getDesiredAllocationMethod),
which mixes through the same normalized matrix and differs only at saturation.

Assumptions:

* The allocated command reaches the TH3SEUS motor unchanged: PX4's motor output
  applies no thrust model (THR_MDL_FAC = 0, see
  src/lib/mixer_module/functions/FunctionMotors.hpp). TH3SEUS's thrust grows
  with the command squared, which is the motor THR_MDL_FAC = 1 describes;
  comparing with logs flown at another THR_MDL_FAC is left to the comparison.
* PX4 runs a symmetric quad X in TH3SEUS's motor order 1 FR (CCW), 2 BL (CCW),
  3 FL (CW), 4 BR (CW). That is PX4's quad X rotor order 0 to 3, and TH3SEUS
  takes PX4's actuator channels 0 to 3 as motors 1 to 4 without remapping
  (src/quadrotor.rs:12-26).
* Predictions are static, at the nominal pack voltage: battery sag and thermal
  derating, which the daemon applies in flight, are not modelled.
"""

from __future__ import annotations

import math
from dataclasses import dataclass

import numpy as np

HITL_PHYSICS_VERSION = "0.13.1"

AXES = ("roll", "pitch", "yaw")

# Mass and electrical defaults (hitl-physics src/build.rs, src/config.rs).
PROP_WEIGHT_DEFAULT_G = 3.0  # src/build.rs:636, each propeller when absent
ESC_TOTAL_WEIGHT_DEFAULT_G = 12.0  # src/build.rs:637, all ESCs together when absent
GPS_WEIGHT_DEFAULT_G = 0.0  # src/build.rs:638, no GPS module
MOTOR_COUNT = 4.0  # src/build.rs:640-641, four identical motors and propellers
VOLTS_PER_CELL = 3.7  # src/build.rs:114-116, BatterySpec::nominal_voltage
ESC_BURST_FACTOR = 1.2  # src/build.rs:534-537, burst = continuous * 1.2 when absent
PACK_SHARING_MOTORS = 4.0  # src/build.rs:547, pack current shared by four motors
MAH_PER_AH = 1000.0  # src/build.rs:547
GRAMS_PER_KG = 1000.0  # src/build.rs:646 and src/config.rs:210
MOTOR_NO_LOAD_DEFAULT_A = 0.5  # src/config.rs:250, when the build gives none

# Motor lag, tau = 0.02 + (1500 / kv) * 0.01 (src/config.rs:199).
TAU_BASE_S = 0.02
TAU_KV_REFERENCE = 1500.0
TAU_KV_SCALE_S = 0.01

# Winding resistance when absent, 0.05 + 0.02 / max(kv / 1000, 0.5)
# (src/config.rs:219-220).
RESISTANCE_BASE_OHM = 0.05
RESISTANCE_SCALE_OHM = 0.02
RESISTANCE_KV_UNIT = 1000.0
RESISTANCE_PROXY_FLOOR = 0.5

# Legacy inertia (src/config.rs:201-215).
INCH_M = 0.0254  # src/config.rs:201
ARM_FACTOR = 1.1  # src/config.rs:201, arm = 1.1 x propeller diameter
FRAME_INERTIA_FACTOR = 0.17  # src/config.rs:212
IZZ_MOTOR_FACTOR = 2.0  # src/config.rs:215

# Full-throttle speed (src/config.rs:243-313).
GRAVITY_M_S2 = 9.80665  # src/config.rs:243
SECONDS_PER_MINUTE = 60.0  # src/config.rs:249, Kt = 60 / (2 * pi * kv)
KQ_FLOOR = 1e-12  # src/config.rs:288, below it the speed is the no-load speed
RPM_TO_RAD_S_DIVISOR = 30.0  # src/config.rs:289, rad/s = rpm * pi / 30

# Propeller coefficients (src/prop_coefficients.rs).
AIR_DENSITY = 1.225  # src/prop_coefficients.rs:30
K_CT = 0.003651555  # src/prop_coefficients.rs:213
KQ_KT_SCALE = 0.47625  # src/prop_coefficients.rs:215
PITCH_BASE = 0.7  # src/prop_coefficients.rs:227 and 243-244
PITCH_SLOPE_PER_IN = 0.06  # src/prop_coefficients.rs:227 and 243-244
KQ_KT_BASE = 0.010  # src/prop_coefficients.rs:231
KQ_KT_PER_PITCH_IN = 0.002  # src/prop_coefficients.rs:231
KQ_KT_PER_BLADE = 0.002  # src/prop_coefficients.rs:231
BLADE_REFERENCE = 2.0  # src/prop_coefficients.rs:266, (blades / 2) ^ 0.7
BLADE_EXPONENT = 0.7  # src/prop_coefficients.rs:266
RESCALE_FLOOR = 0.1  # src/prop_coefficients.rs:245 and 270
EXACT_MATCH_TOLERANCE = 1e-3  # src/prop_coefficients.rs:143-144
NEAREST_DIAMETER_WEIGHT = 4.0  # src/prop_coefficients.rs:173
NEAREST_BLADE_WEIGHT = 1.5  # src/prop_coefficients.rs:173

# (diameter_in, pitch_in, blade_count, ct_static, cq_static) in file order,
# hitl-physics data/prop_coefficients.csv:64-115. The order matters: lookup
# keeps the first of equally near rows.
PROP_COEFFICIENT_TABLE: tuple[tuple[float, float, int, float, float], ...] = (
    (7.5, 3.7, 3, 4.471700e-3, 2.615944e-4),
    (1.57, 1.22, 4, 4.586598e-3, 1.119627e-3),
    (2.5, 1.6, 3, 3.860600e-3, 5.559264e-4),
    (2.5, 1.6, 4, 4.721847e-3, 7.507737e-4),
    (2.5, 2.5, 2, 3.103822e-3, 4.422946e-4),
    (3.0, 1.6, 3, 3.860600e-3, 4.632720e-4),
    (3.0, 2.0, 3, 3.977000e-3, 4.971250e-4),
    (3.0, 2.5, 3, 4.122500e-3, 5.410781e-4),
    (3.0, 3.0, 3, 4.268000e-3, 5.868500e-4),
    (3.0, 3.0, 2, 3.213368e-3, 4.016711e-4),
    (3.0, 4.0, 3, 4.559000e-3, 6.838500e-4),
    (3.5, 2.0, 3, 3.977000e-3, 4.261071e-4),
    (3.5, 3.0, 3, 4.268000e-3, 5.030143e-4),
    (4.0, 2.5, 3, 4.122500e-3, 4.058086e-4),
    (4.0, 4.0, 3, 4.559000e-3, 5.128875e-4),
    (4.0, 4.0, 2, 3.432462e-3, 3.539726e-4),
    (4.0, 4.5, 3, 4.704500e-3, 5.513086e-4),
    (4.0, 5.0, 3, 4.850000e-3, 5.910937e-4),
    (5.0, 3.0, 3, 4.268000e-3, 3.521100e-4),
    (5.0, 3.0, 2, 3.213368e-3, 2.410026e-4),
    (5.0, 4.0, 3, 4.559000e-3, 4.103100e-4),
    (5.0, 4.0, 2, 3.432462e-3, 2.831781e-4),
    (5.0, 4.3, 3, 4.646300e-3, 4.286212e-4),
    (5.0, 4.5, 3, 4.704500e-3, 4.410469e-4),
    (5.0, 4.5, 2, 3.542008e-3, 3.054982e-4),
    (5.0, 4.5, 4, 5.754010e-3, 5.825935e-4),
    (5.0, 5.0, 3, 4.850000e-3, 4.728750e-4),
    (5.0, 5.1, 3, 4.879100e-3, 4.793716e-4),
    (5.0, 6.0, 3, 5.141000e-3, 5.398050e-4),
    (5.0, 4.5, 5, 6.726786e-3, 7.315379e-4),
    (6.0, 3.0, 3, 4.268000e-3, 2.934250e-4),
    (6.0, 3.0, 2, 3.213368e-3, 2.008355e-4),
    (6.0, 4.0, 3, 4.559000e-3, 3.419250e-4),
    (6.0, 4.0, 2, 3.432462e-3, 2.359817e-4),
    (6.0, 4.5, 3, 4.704500e-3, 3.675391e-4),
    (6.0, 5.5, 3, 4.995500e-3, 4.214953e-4),
    (7.0, 2.5, 3, 4.122500e-3, 2.318906e-4),
    (7.0, 3.5, 2, 3.322915e-3, 1.869140e-4),
    (7.0, 3.5, 3, 4.413500e-3, 2.719031e-4),
    (7.0, 4.0, 2, 3.432462e-3, 2.022701e-4),
    (7.0, 4.0, 3, 4.559000e-3, 2.930786e-4),
    (7.0, 5.0, 2, 3.651555e-3, 2.347428e-4),
    (8.0, 4.0, 2, 3.432462e-3, 1.769863e-4),
    (8.0, 4.5, 3, 4.704500e-3, 2.756543e-4),
    (8.0, 5.0, 2, 3.651555e-3, 2.054000e-4),
    (8.0, 5.0, 3, 4.850000e-3, 2.955469e-4),
    (8.0, 6.0, 2, 3.870648e-3, 2.358676e-4),
    (10.0, 4.5, 2, 3.542008e-3, 1.527491e-4),
    (10.0, 5.0, 2, 3.651555e-3, 1.643200e-4),
    (10.0, 5.0, 3, 4.850000e-3, 2.364375e-4),
    (10.0, 5.5, 2, 3.761102e-3, 1.763016e-4),
    (10.0, 6.0, 2, 3.870648e-3, 1.886941e-4),
)

# TH3SEUS quad-X torque, motor order 1 FR, 2 BL, 3 FL, 4 BR (src/quadrotor.rs).
COS_45 = 1.0 / math.sqrt(2.0)  # src/quadrotor.rs:108
ROLL_THRUST_SIGNS = (-1.0, 1.0, 1.0, -1.0)  # src/quadrotor.rs:109
PITCH_THRUST_SIGNS = (1.0, -1.0, 1.0, -1.0)  # src/quadrotor.rs:113
YAW_REACTION_SIGNS = (1.0, 1.0, -1.0, -1.0)  # src/quadrotor.rs:122

# PX4 quad X rotors (PX, PY, KM) in rotor order 0 to 3, which is TH3SEUS motor
# order 1 FR, 2 BL, 3 FL, 4 BR: PX4 v1.14.0
# ROMFS/px4fmu_common/init.d/airframes/1001_rc_quad_x.hil, KM 0.05 being the
# CA_ROTORn_KM default (src/modules/control_allocator/module.yaml:207-221).
PX4_QUAD_X_ROTORS = (
    (0.15, 0.15, 0.05),
    (-0.15, -0.15, 0.05),
    (0.15, -0.15, -0.05),
    (-0.15, 0.15, -0.05),
)
PX4_ROTOR_CT = 6.5  # CA_ROTORn_CT default, module.yaml:192-205 at v1.14.0
PX4_ROTOR_AXIS = (0.0, 0.0, -1.0)  # CA_ROTORn_AX/AY/AZ defaults, module.yaml:158-190
# ControlAllocationPseudoInverse.cpp:83-87 and 160-168 at v1.14.0: entries
# below this count as zero and are set to zero.
PX4_ZERO_THRESHOLD = 1e-3
# ControlAllocationPseudoInverse.cpp:145-158: a scale at or below FLT_EPSILON is not applied.
PX4_FLT_EPSILON = float(np.finfo(np.float32).eps)


@dataclass(frozen=True, kw_only=True)
class Build:
    """Component values of a quadrotor build, named after hitl-physics'
    `BuildSpec` (src/build.rs:28-117 and 477-487). All four motors and
    propellers are identical. A value left as None takes hitl-physics' default:
    winding resistance from KV, 0.5 A no-load current, ESC burst 1.2 x
    continuous, 3.0 g per propeller, 12.0 g for all ESCs together, no GPS, and a
    pack voltage of cells x 3.7 V."""

    frame_weight_g: float
    motor_kv: float
    motor_weight_g: float
    motor_resistance_ohm: float | None = None
    motor_no_load_amps: float | None = None
    esc_continuous_amps: float
    esc_burst_amps: float | None = None
    prop_diameter_in: float
    prop_pitch_in: float
    prop_blade_count: int
    prop_weight_g: float | None = None
    esc_total_weight_g: float | None = None
    battery_cells: int
    battery_capacity_mah: float
    battery_c_rating: float
    battery_weight_g: float
    fc_weight_g: float
    gps_weight_g: float | None = None
    battery_voltage_v: float | None = None

    def __post_init__(self) -> None:
        for name in _POSITIVE_FIELDS:
            _require_number(name, getattr(self, name), positive=True)
        for name in _NON_NEGATIVE_FIELDS:
            _require_number(name, getattr(self, name), positive=False)
        for name in _OPTIONAL_POSITIVE_FIELDS:
            if getattr(self, name) is not None:
                _require_number(name, getattr(self, name), positive=True)
        for name in _OPTIONAL_NON_NEGATIVE_FIELDS:
            if getattr(self, name) is not None:
                _require_number(name, getattr(self, name), positive=False)
        for name in _COUNT_FIELDS:
            _require_count(name, getattr(self, name))


_POSITIVE_FIELDS = (
    "motor_kv",
    "esc_continuous_amps",
    "prop_diameter_in",
    "prop_pitch_in",
    "battery_capacity_mah",
    "battery_c_rating",
)
_NON_NEGATIVE_FIELDS = ("frame_weight_g", "motor_weight_g", "battery_weight_g", "fc_weight_g")
_OPTIONAL_POSITIVE_FIELDS = ("motor_resistance_ohm", "esc_burst_amps", "battery_voltage_v")
_OPTIONAL_NON_NEGATIVE_FIELDS = (
    "motor_no_load_amps",
    "prop_weight_g",
    "esc_total_weight_g",
    "gps_weight_g",
)
_COUNT_FIELDS = ("prop_blade_count", "battery_cells")


def _require_number(name: str, value: float, *, positive: bool) -> None:
    in_range = value > 0.0 if positive else value >= 0.0
    if not (math.isfinite(value) and in_range):
        kind = "positive" if positive else "non-negative"
        raise ValueError(f"{name} must be a finite {kind} number, got {value!r}")


def _require_count(name: str, value: int) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ValueError(f"{name} must be an integer of at least 1, got {value!r}")


def _require_propeller(diameter_in: float, pitch_in: float, blade_count: int) -> None:
    _require_number("diameter_in", diameter_in, positive=True)
    _require_number("pitch_in", pitch_in, positive=True)
    _require_count("blade_count", blade_count)


@dataclass(frozen=True)
class Prediction:
    """What hitl-physics 0.13.1 gives for a build.

    Units: mass kg, inertia kg m^2, motor lag s, kt N/(rad/s)^2,
    kq N m/(rad/s)^2, full-throttle motor speed rad/s, hover command as a
    fraction of the full-scale motor command, and effectiveness in rad/s^2 per
    unit of PX4's normalized torque setpoint."""

    mass_kg: float
    ixx: float
    iyy: float
    izz: float
    tau_motor_s: float
    kt: float
    kq: float
    omega_max_rad_s: float
    hover_command: float
    thrust_to_weight: float
    effectiveness_roll: float
    effectiveness_pitch: float
    effectiveness_yaw: float


def predict(build: Build) -> Prediction:
    """TH3SEUS's predictions for `build`, as hitl-physics 0.13.1 computes them.

    Raises ValueError when the build leaves no current above the motors'
    no-load draw: hitl-physics then gives a full-throttle speed of 0 and an
    infinite hover command."""
    mass_kg = _total_mass_kg(build)
    arm_m, inertia = _legacy_arm_and_inertia(build)
    kt, kq = _kt_kq(build)
    omega_max = _max_motor_speed(build, kq)
    omega_hover = math.sqrt((mass_kg * GRAVITY_M_S2) / (MOTOR_COUNT * kt))
    hover_command = omega_hover / omega_max
    roll, pitch, yaw = _effectiveness(kt, kq, omega_max, hover_command, arm_m, inertia)
    return Prediction(
        mass_kg=mass_kg,
        ixx=inertia[0],
        iyy=inertia[1],
        izz=inertia[2],
        tau_motor_s=TAU_BASE_S + (TAU_KV_REFERENCE / build.motor_kv) * TAU_KV_SCALE_S,
        kt=kt,
        kq=kq,
        omega_max_rad_s=omega_max,
        hover_command=hover_command,
        thrust_to_weight=MOTOR_COUNT * kt * omega_max * omega_max / (mass_kg * GRAVITY_M_S2),
        effectiveness_roll=roll,
        effectiveness_pitch=pitch,
        effectiveness_yaw=yaw,
    )


def unit_torque_motor_deltas(axis: str) -> np.ndarray:
    """Motor-command changes, motor order 1 FR, 2 BL, 3 FL, 4 BR, that PX4's
    control allocation gives a quad X for a unit normalized torque setpoint on
    `axis` ("roll", "pitch" or "yaw"). Returns a new array on every call."""
    if axis not in AXES:
        raise ValueError(f"axis must be one of {AXES}, got {axis!r}")
    return _px4_normalized_torque_mix()[:, AXES.index(axis)].copy()


def prop_coefficients(diameter_in: float, pitch_in: float, blade_count: int) -> tuple[float, float]:
    """(ct_static, cq_static) as hitl-physics' `prop_coefficients::lookup`
    resolves them (src/prop_coefficients.rs:129-190): the average of the
    table rows matching the geometry, or else the nearest row rescaled to the
    requested pitch and blade count. The table is never empty, so lookup's
    closed-form fallback is never reached."""
    _require_propeller(diameter_in, pitch_in, blade_count)
    exact = [
        row
        for row in PROP_COEFFICIENT_TABLE
        if abs(row[0] - diameter_in) < EXACT_MATCH_TOLERANCE
        and abs(row[1] - pitch_in) < EXACT_MATCH_TOLERANCE
        and row[2] == blade_count
    ]
    if exact:
        return sum(row[3] for row in exact) / len(exact), sum(row[4] for row in exact) / len(exact)
    nearest = min(
        PROP_COEFFICIENT_TABLE,
        key=lambda row: _lookup_distance(row, diameter_in, pitch_in, blade_count),
    )
    pitch_scale = max(_pitch_factor(pitch_in) / _pitch_factor(nearest[1]), RESCALE_FLOOR)
    blade_scale = max(_blade_factor(blade_count) / _blade_factor(nearest[2]), RESCALE_FLOOR)
    return nearest[3] * pitch_scale * blade_scale, nearest[4] * pitch_scale * blade_scale


def prop_coefficients_from_basis(
    diameter_in: float, pitch_in: float, blade_count: int
) -> tuple[float, float]:
    """(ct_static, cq_static) from hitl-physics' closed-form basis
    (`derive_from_basis`, src/prop_coefficients.rs:222-239), the formula each
    row of the coefficient table caches."""
    _require_propeller(diameter_in, pitch_in, blade_count)
    ct = K_CT * _pitch_factor(pitch_in) * _blade_factor(blade_count)
    kq_kt = KQ_KT_SCALE * (
        KQ_KT_BASE + KQ_KT_PER_PITCH_IN * pitch_in + KQ_KT_PER_BLADE * blade_count
    )
    return ct, ct * kq_kt / (diameter_in * INCH_M)


def _pitch_factor(pitch_in: float) -> float:
    # src/prop_coefficients.rs:227 and 241-246
    return PITCH_BASE + PITCH_SLOPE_PER_IN * pitch_in


def _blade_factor(blade_count: int) -> float:
    # src/prop_coefficients.rs:265-267
    return (blade_count / BLADE_REFERENCE) ** BLADE_EXPONENT


def _lookup_distance(
    row: tuple[float, float, int, float, float], diameter_in: float, pitch_in: float, blade_count: int
) -> float:
    # src/prop_coefficients.rs:168-173
    return (
        (NEAREST_DIAMETER_WEIGHT * (row[0] - diameter_in)) ** 2
        + (row[1] - pitch_in) ** 2
        + (NEAREST_BLADE_WEIGHT * (row[2] - blade_count)) ** 2
    )


def _kt_kq(build: Build) -> tuple[float, float]:
    # src/build.rs:520-523 and src/prop_coefficients.rs:274-279
    ct, cq = prop_coefficients(build.prop_diameter_in, build.prop_pitch_in, build.prop_blade_count)
    diameter_m = build.prop_diameter_in * INCH_M
    return ct * AIR_DENSITY * diameter_m**4, cq * AIR_DENSITY * diameter_m**5


def _or_default(value: float | None, default: float) -> float:
    return default if value is None else value


def _total_mass_kg(build: Build) -> float:
    # src/build.rs:635-647
    grams = (
        build.frame_weight_g
        + MOTOR_COUNT * build.motor_weight_g
        + MOTOR_COUNT * _or_default(build.prop_weight_g, PROP_WEIGHT_DEFAULT_G)
        + _or_default(build.esc_total_weight_g, ESC_TOTAL_WEIGHT_DEFAULT_G)
        + build.battery_weight_g
        + build.fc_weight_g
        + _or_default(build.gps_weight_g, GPS_WEIGHT_DEFAULT_G)
    )
    return grams / GRAMS_PER_KG


def _legacy_arm_and_inertia(build: Build) -> tuple[float, tuple[float, float, float]]:
    # src/config.rs:201-215: frame and motor weights only, motors at the arm tips
    arm_m = build.prop_diameter_in * INCH_M * ARM_FACTOR
    motor_inertia = MOTOR_COUNT * (build.motor_weight_g / GRAMS_PER_KG) * arm_m * arm_m
    frame_inertia = (build.frame_weight_g / GRAMS_PER_KG) * arm_m * arm_m * FRAME_INERTIA_FACTOR
    ixx = motor_inertia + frame_inertia
    return arm_m, (ixx, ixx, motor_inertia * IZZ_MOTOR_FACTOR + frame_inertia)


def _battery_voltage(build: Build) -> float:
    # src/build.rs:114-116 and 508
    if build.battery_voltage_v is not None:
        return build.battery_voltage_v
    return build.battery_cells * VOLTS_PER_CELL


def _motor_resistance_ohm(build: Build) -> float:
    # src/config.rs:219-220, overridden by the build at src/build.rs:529-531
    if build.motor_resistance_ohm is not None:
        return build.motor_resistance_ohm
    stator_proxy = max(build.motor_kv / RESISTANCE_KV_UNIT, RESISTANCE_PROXY_FLOOR)
    return RESISTANCE_BASE_OHM + RESISTANCE_SCALE_OHM / stator_proxy


def _max_current_per_motor_a(build: Build) -> float:
    # src/build.rs:532-548: ESC burst rating or the pack's share, whichever is lower
    burst = _or_default(build.esc_burst_amps, build.esc_continuous_amps * ESC_BURST_FACTOR)
    pack = build.battery_capacity_mah / MAH_PER_AH * build.battery_c_rating / PACK_SHARING_MOTORS
    return min(burst, pack)


def _max_motor_speed(build: Build, kq: float) -> float:
    # src/config.rs:283-313
    voltage = _battery_voltage(build)
    kt_electrical = SECONDS_PER_MINUTE / (2.0 * math.pi * build.motor_kv)
    if kq < KQ_FLOOR:
        return build.motor_kv * voltage * math.pi / RPM_TO_RAD_S_DIVISOR
    resistance = _motor_resistance_ohm(build)
    b = kt_electrical * kt_electrical / resistance
    c = kt_electrical * voltage / resistance
    omega_voltage = (-b + math.sqrt(b * b + 4.0 * kq * c)) / (2.0 * kq)
    # src/config.rs:250, overridden by the build at src/build.rs:526-528
    no_load = _or_default(build.motor_no_load_amps, MOTOR_NO_LOAD_DEFAULT_A)
    max_current = _max_current_per_motor_a(build)
    usable = max(max_current - no_load, 0.0)
    if usable <= 0.0:
        raise ValueError(
            f"the build has no usable motor current: {max_current} A available per motor "
            f"against {no_load} A no-load draw"
        )
    return min(omega_voltage, math.sqrt(kt_electrical * usable / kq))


def _effectiveness(
    kt: float,
    kq: float,
    omega_max: float,
    hover_command: float,
    arm_m: float,
    inertia: tuple[float, float, float],
) -> tuple[float, float, float]:
    """Angular acceleration per unit normalized torque setpoint on each axis."""
    # d(k * (u * omega_max)^2)/du at the hover command (src/motor.rs:34-46 and 89-106)
    thrust_slope = 2.0 * kt * omega_max * omega_max * hover_command
    reaction_slope = 2.0 * kq * omega_max * omega_max * hover_command
    mix = _px4_normalized_torque_mix()
    torques = [
        _body_torque(mix[:, index], thrust_slope, reaction_slope, arm_m)[index]
        for index in range(len(AXES))
    ]
    roll, pitch, yaw = (float(torque / inertia[index]) for index, torque in enumerate(torques))
    return roll, pitch, yaw


def _body_torque(
    motor_deltas: np.ndarray, thrust_slope: float, reaction_slope: float, arm_m: float
) -> np.ndarray:
    # src/quadrotor.rs:106-122: roll, pitch and yaw torque for the command changes
    thrust = thrust_slope * motor_deltas
    reaction = reaction_slope * motor_deltas
    lever = arm_m * COS_45
    return np.array(
        [
            lever * float(np.dot(ROLL_THRUST_SIGNS, thrust)),
            lever * float(np.dot(PITCH_THRUST_SIGNS, thrust)),
            float(np.dot(YAW_REACTION_SIGNS, reaction)),
        ]
    )


def _px4_effectiveness() -> np.ndarray:
    """PX4's rotor effectiveness matrix, rows roll, pitch, yaw torque then x, y,
    z thrust, one column per rotor (ActuatorEffectivenessRotors.cpp)."""
    axis = np.array(PX4_ROTOR_AXIS)
    columns = []
    for px, py, km in PX4_QUAD_X_ROTORS:
        moment = PX4_ROTOR_CT * np.cross(np.array([px, py, 0.0]), axis) - PX4_ROTOR_CT * km * axis
        columns.append(np.concatenate([moment, PX4_ROTOR_CT * axis]))
    return np.column_stack(columns)


def _px4_torque_scales(mix: np.ndarray) -> tuple[float, float, float]:
    """Roll, pitch and yaw scales of updateControlAllocationMatrixScale."""

    def norm_scale(column: np.ndarray) -> float:
        non_zero = int(np.count_nonzero(np.abs(column) > PX4_ZERO_THRESHOLD))
        return math.sqrt(float(column @ column) / (non_zero / 2.0)) if non_zero else 1.0

    roll_pitch = max(norm_scale(mix[:, 0]), norm_scale(mix[:, 1]))
    return roll_pitch, roll_pitch, float(mix[:, 2].max())


def _px4_normalized_torque_mix() -> np.ndarray:
    """Roll, pitch and yaw columns of PX4's normalized allocation matrix,
    one row per motor (normalizeControlAllocationMatrix)."""
    mix = np.linalg.pinv(_px4_effectiveness())[:, : len(AXES)]
    roll_scale, pitch_scale, yaw_scale = _px4_torque_scales(mix)
    columns = [mix[:, 0], mix[:, 1], mix[:, 2]]
    if roll_scale > PX4_FLT_EPSILON:
        columns[0], columns[1] = columns[0] / roll_scale, columns[1] / pitch_scale
    if yaw_scale > PX4_FLT_EPSILON:
        columns[2] = columns[2] / yaw_scale
    normalized = np.column_stack(columns)
    return np.where(np.abs(normalized) < PX4_ZERO_THRESHOLD, 0.0, normalized)


REFERENCE_BUILDS: dict[str, Build] = {
    # BuildSpec::tinyhawk_3, hitl-physics src/build.rs:784
    "tinyhawk_3": Build(
        frame_weight_g=19.56,
        motor_kv=15_000.0,
        motor_weight_g=2.5,
        esc_continuous_amps=5.0,
        esc_burst_amps=5.0,
        prop_diameter_in=1.57,
        prop_pitch_in=1.22,
        prop_blade_count=4,
        prop_weight_g=0.61,
        esc_total_weight_g=0.0,
        battery_cells=1,
        battery_capacity_mah=450.0,
        battery_c_rating=80.0,
        battery_weight_g=12.5,
        fc_weight_g=0.0,
    ),
    # BuildSpec::impulse_apex_5, hitl-physics src/build.rs:858
    "impulse_apex_5": Build(
        frame_weight_g=206.4,
        motor_kv=1700.0,
        motor_weight_g=33.0,
        motor_resistance_ohm=0.065,
        motor_no_load_amps=0.5,
        esc_continuous_amps=45.0,
        esc_burst_amps=55.0,
        prop_diameter_in=5.0,
        prop_pitch_in=4.3,
        prop_blade_count=3,
        prop_weight_g=4.5,
        esc_total_weight_g=13.6,
        battery_cells=4,
        battery_capacity_mah=1300.0,
        battery_c_rating=75.0,
        battery_weight_g=164.0,
        fc_weight_g=8.0,
    ),
    # BuildSpec::geprc_mark5, hitl-physics src/build.rs:927
    "geprc_mark5": Build(
        frame_weight_g=228.2,
        motor_kv=1960.0,
        motor_weight_g=29.5,
        motor_no_load_amps=0.6,
        esc_continuous_amps=50.0,
        esc_burst_amps=55.0,
        prop_diameter_in=5.0,
        prop_pitch_in=4.0,
        prop_blade_count=3,
        prop_weight_g=3.7,
        esc_total_weight_g=14.0,
        battery_cells=6,
        battery_capacity_mah=1400.0,
        battery_c_rating=120.0,
        battery_weight_g=226.0,
        fc_weight_g=10.0,
    ),
    # BuildSpec::iflight_chimera_7, hitl-physics src/build.rs:1003
    "iflight_chimera_7": Build(
        frame_weight_g=379.9,
        motor_kv=1250.0,
        motor_weight_g=60.7,
        motor_resistance_ohm=0.06546,
        motor_no_load_amps=0.8,
        esc_continuous_amps=55.0,
        esc_burst_amps=65.0,
        prop_diameter_in=7.5,
        prop_pitch_in=3.7,
        prop_blade_count=3,
        prop_weight_g=8.0,
        esc_total_weight_g=15.3,
        battery_cells=6,
        battery_capacity_mah=8000.0,
        battery_c_rating=17.5,
        battery_weight_g=840.0,
        fc_weight_g=12.0,
        gps_weight_g=23.0,
    ),
}
