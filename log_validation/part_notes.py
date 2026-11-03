"""The hitl-physics 0.13.1 arm comments, classified.

Each mirrored comment (`Variant.comment`) is split into three kinds of text:

- a fact about the part (no fields), which stays true whatever the table holds;
- a figure for one or more profile fields (for example a noise density), which
  describes the 0.13.1 value of those fields;
- a comparison ("best in class", "~17x quieter than MPU6000"), a claim about
  table values that a measured value can contradict.

The rule: a comparison is never written. A fact is always written. A figure is
written only when none of its fields has a proposed value in the arm. When every
note is written and there is no comparison, the mirrored comment is written
unchanged; otherwise the written notes are joined with "; ".

A mirrored comment that is not fully classified raises ValueError, so a mirror
bump that changes a comment fails until the comment is classified.
"""

from dataclasses import dataclass
from types import MappingProxyType
from typing import Mapping

from log_validation.th3seus_model import HITL_PHYSICS_VERSION
from log_validation.th3seus_parts import variant

G = "gyro_noise_density"
A = "accel_noise_density"
N = "noise_sigma_gauss"
H = "horizontal_noise_sigma_m"
R = "update_rate_hz"
D = "delay_ms"

_LEFTOVER = frozenset(" ,;:-")


@dataclass(frozen=True)
class Note:
    text: str
    fields: tuple[str, ...] = ()


def _ro(table: dict) -> Mapping:
    return MappingProxyType({k: MappingProxyType(v) for k, v in table.items()})


NOTES: Mapping[str, Mapping[str, tuple[Note, ...]]] = _ro({
    "imu": {
        "Mpu6000": (
            Note("0.05 deg/s/sqrt(Hz)", (G,)),
            Note("650 ug/sqrt(Hz)", (A,)),
            Note("legacy reference", (G, A)),
            Note("used in tests"),
        ),
        "Mpu6500": (),
        "Icm20689": (Note("0.005 deg/s/sqrt(Hz)", (G,)), Note("80 ug/sqrt(Hz)", (A,))),
        "Icm42688p": (
            Note("0.0028 deg/s/sqrt(Hz)", (G,)),
            Note("70 ug/sqrt(Hz)", (A,)),
            Note("modern flagship"),
        ),
        "Bmi270": (Note("0.008 deg/s/sqrt(Hz)", (G,)), Note("250 ug/sqrt(Hz)", (A,))),
        "Lsm6dso": (Note("0.0035 deg/s/sqrt(Hz)", (G,)), Note("60 ug/sqrt(Hz)", (A,))),
    },
    "baro": {
        "Dps310": (),
    },
    "mag": {
        "Hmc5883": (Note("~0.6 mGauss noise", (N,)),),
        "Qmc5883": (Note("~0.5 mGauss", (N,)), Note("cheap, temperature-unstable")),
        "Ist8310": (Note("~0.3 mGauss", (N,)),),
        "Rm3100": (Note("~0.013 mGauss", (N,)),),
        "Lis3mdl": (Note("~0.32 mGauss", (N,)),),
    },
    "gps": {
        "UbloxM8N": (
            Note("u-blox NEO-M8N"),
            Note("2.5 m CEP", (H,)),
            Note("18 Hz max (5 Hz default)", (R,)),
            Note("GPS+GLONASS"),
        ),
        "UbloxM9N": (
            Note("u-blox NEO-M9N"),
            Note("1.5 m CEP", (H,)),
            Note("25 Hz max", (R,)),
            Note("4-constellation"),
        ),
        "UbloxM10": (Note("u-blox M10"), Note("lower power, single-band")),
        "UbloxF9P": (
            Note("u-blox ZED-F9P"),
            Note("0.01 m RTK fixed", (H,)),
            Note("1.5 m standalone", (H,)),
            Note("8 Hz typical", (R,)),
        ),
        "SeptentrioMosaic": (
            Note("Septentrio Mosaic-X5"),
            Note("0.006 m RTK", (H,)),
            Note("100 Hz", (R,)),
            Note("survey-grade"),
        ),
        "Here3": (
            Note("CubePilot Here3 (CAN GPS)"),
            Note("built on u-blox M9N"),
            Note("delay_ms 100.0 is CAN bus latency", (D,)),
        ),
    },
})

COMPARISONS: Mapping[str, Mapping[str, tuple[str, ...]]] = _ro({
    "imu": {
        "Mpu6500": ("Slightly noisier than MPU6000",),
        "Icm42688p": ("~17x quieter than MPU6000",),
    },
    "baro": {"Dps310": ("best in class",)},
    "mag": {"Rm3100": ("high-performance fluxgate-class",)},
    "gps": {"UbloxM10": ("similar accuracy to M9N",)},
})


def _unclassified(part_type: str, name: str) -> ValueError:
    return ValueError(f"{part_type}: the hitl-physics {HITL_PHYSICS_VERSION} comment of {name} "
                      "is not fully classified in part_notes.py")


def _check_complete(
    part_type: str, name: str, comment: str, notes: tuple[Note, ...], comparisons: tuple[str, ...]
) -> None:
    rest = comment
    for text in (*(n.text for n in notes), *comparisons):
        if text not in rest:
            raise _unclassified(part_type, name)
        rest = rest.replace(text, "", 1)
    if not set(rest) <= _LEFTOVER:
        raise _unclassified(part_type, name)


def existing_arm_comment(part_type: str, name: str, measured: frozenset[str]) -> str:
    """The comment to write for an existing variant's arm; "" when there is none.

    `measured` holds the fields that have a proposed value in the arm. Not for
    `Other`. Raises ValueError when the mirrored comment is not fully classified.
    """
    comment = variant(part_type, name).comment
    entry = NOTES.get(part_type, {}).get(name)
    if entry is None:
        if not comment:
            return ""
        raise _unclassified(part_type, name)
    comparisons = COMPARISONS.get(part_type, {}).get(name, ())
    _check_complete(part_type, name, comment, entry, comparisons)
    written = [n.text for n in entry if not set(n.fields) & measured]
    if not comparisons and len(written) == len(entry):
        return comment
    return "; ".join(written)
