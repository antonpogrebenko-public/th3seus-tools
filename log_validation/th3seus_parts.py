"""TH3SEUS's current part tables and part-name parsers, mirrored and pinned.

Two Rust sources are mirrored, so later code can compare values derived from
flight logs with the ones TH3SEUS uses today and emit updated Rust:

* the part tables of hitl-physics 0.13.1 (`HITL_PHYSICS_VERSION`), commit
  9d2573b, src/build.rs lines 195-462: the enums `ImuChip`, `MagChip`,
  `BaroChip` and `GpsChipset` and their profile functions `imu_profile`,
  `mag_profile`, `baro_profile` and `gps_profile`. Variant order, field order
  and every literal are copied as written; comments are ASCII renderings
  (deg, sqrt(Hz), ug);
* the name parsers of hitl-daemon 0.27.0 (`HITL_DAEMON_VERSION`), commit
  54d9f79, crates/websocket/src/build_config.rs: `parse_imu_chip`,
  `parse_baro_chip`, `parse_mag_chip` and `parse_gps_chipset`.

Neither repository has a tag at these commits; the pins are the commits. The
four parsers are unchanged through hitl-daemon commit f821d49. The daemon's
sensor loop runs its mag and baro on `SENSOR_50HZ_PERIOD_US = 20_000`
(crates/simulation/src/loop_runner.rs, hitl-daemon 54d9f79), the period a
later task uses for rate conversion. The sibling hitl-physics and hitl-daemon
changes update these pins when they land.

Where a build.rs `Other` arm delegates to another variant's profile (IMU Other
and GPS Other), the variant stores the literals it resolves to in `fields` and
the Rust expression in `delegate`; an emitter that proposes nothing for Other
writes `delegate` and so keeps 0.13.1's structure.

The daemon matches names after `normalize`: lowercase with `-`, `_` and spaces
removed. IMU, baro and mag names must then equal an alias exactly; GPS names
are matched by substring rules tried in order.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass
from types import MappingProxyType

from log_validation.th3seus_model import HITL_PHYSICS_VERSION

__all__ = [
    "HITL_DAEMON_VERSION",
    "HITL_PHYSICS_VERSION",
    "PART_TYPES",
    "PARTS",
    "OTHER",
    "Variant",
    "PartType",
    "Classified",
    "normalize",
    "daemon_parse",
    "classify_log_part",
    "candidate_variant_name",
    "variant",
    "current_literal",
    "current_value",
    "fix_class",
]

HITL_DAEMON_VERSION = "0.27.0"
PART_TYPES = ("imu", "baro", "mag", "gps")
OTHER = "Other"

# GPS fix classes. The strings are the part tables' classes; FIX_TYPE_OF_CLASS
# gives the PX4 `fix_type` of each (3 = 3D fix, 6 = RTK fixed).
FIX_CLASS_STANDALONE_3D = "standalone_3d"
FIX_CLASS_RTK_FIXED = "rtk_fixed"
FIX_TYPE_OF_CLASS = MappingProxyType({FIX_CLASS_STANDALONE_3D: 3, FIX_CLASS_RTK_FIXED: 6})
RTK_ENTRIES = frozenset({"UbloxF9P", "SeptentrioMosaic"})

KIND_EXISTING = "existing"
KIND_CANDIDATE = "candidate"
KIND_OTHER_ONLY = "other_only"
KIND_EXCLUDED = "excluded"

# The tables below are read-only (MappingProxyType): they mirror pinned sources.
# PX4 GPS names the daemon's substring rules miss (UBX_8 has no "m8").
LOG_NAME_VARIANTS = MappingProxyType({
    "gps": MappingProxyType({
        "UBX_8": "UbloxM8N",
        "UBX_9": "UbloxM9N",
        "UBX_10": "UbloxM10",
        "UBX_F9P": "UbloxF9P",
        "SBF": "SeptentrioMosaic",
    }),
})
# Names that identify a protocol or bus, not a part.
GENERIC_LOG_NAMES = MappingProxyType({
    "imu": frozenset({"UAVCAN"}),
    "baro": frozenset({"UAVCAN"}),
    "mag": frozenset({"UAVCAN"}),
    "gps": frozenset({"UAVCAN", "UBX", "NMEA"}),
})
SIMULATED_LOG_NAMES = MappingProxyType(
    {"imu": "SIM", "baro": "BAROSIM", "mag": "MAGSIM", "gps": "SIM"})
UNKNOWN_PREFIX = "UNKNOWN_"
# A candidate variant name splits a log name on every run of these.
_NAME_SEPARATORS = re.compile(r"[^A-Za-z0-9]+")

# hitl-daemon 0.27.0 exact aliases (after normalize) for the three exact parsers.
_ALIASES = MappingProxyType({
    "imu": MappingProxyType({
        "mpu6000": "Mpu6000", "mpu6000p": "Mpu6000",
        "mpu6500": "Mpu6500", "mpu6500p": "Mpu6500",
        "icm20689": "Icm20689", "icm20689p": "Icm20689",
        "icm42688p": "Icm42688p", "icm42688": "Icm42688p",
        "bmi270": "Bmi270",
        "lsm6dso": "Lsm6dso", "lsm6dso32": "Lsm6dso",
    }),
    "baro": MappingProxyType({
        "bmp280": "Bmp280",
        "bmp388": "Bmp388", "bmp390": "Bmp388",
        "dps310": "Dps310",
        "spl06": "Spl06", "spl06001": "Spl06",
        "ms5611": "Ms5611", "ms561101ba03": "Ms5611",
    }),
    "mag": MappingProxyType({
        "hmc5883": "Hmc5883", "hmc5883l": "Hmc5883",
        "qmc5883": "Qmc5883", "qmc5883l": "Qmc5883",
        "ist8310": "Ist8310",
        "rm3100": "Rm3100",
        "lis3mdl": "Lis3mdl",
        "none": "None", "": "None",
    }),
})
# parse_gps_chipset: (substrings, variant), first rule with a hit wins.
_GPS_RULES = (
    (("f9p", "zedf9p"), "UbloxF9P"),
    (("m10",), "UbloxM10"),
    (("m9",), "UbloxM9N"),
    (("m8",), "UbloxM8N"),
    (("mosaic", "septentrio"), "SeptentrioMosaic"),
    (("here3",), "Here3"),
)


@dataclass(frozen=True)
class Variant:
    name: str
    fields: tuple[tuple[str, str], ...]  # (profile field, Rust literal as in build.rs 0.13.1), build.rs order
    comment: str                         # ASCII arm comment, "" for none
    fix_class: str | None = None         # gps only
    delegate: str = ""                   # Rust expression when the arm delegates, "" when it writes literals

    def value(self, field: str) -> float:
        return float(dict(self.fields)[field])


@dataclass(frozen=True)
class PartType:
    key: str
    enum: str
    profile: str
    profile_fn: str
    optional: bool                         # profile fn returns Option<...> (mag)
    parser: str                            # hitl-daemon parser name
    measured: tuple[tuple[str, str], ...]  # (profile field, per-log measurement key)
    variants: tuple[Variant, ...]          # build.rs order, Other last; MagChip::None not here
    no_profile_variants: tuple[str, ...]   # ("None",) for mag
    doc: tuple[str, ...]                   # ASCII doc lines of the profile fn


@dataclass(frozen=True)
class Classified:
    kind: str   # "existing", "candidate", "other_only" or "excluded"
    entry: str


def _imu(name, gyro, accel, sigma, tau, comment="", delegate=""):
    return Variant(
        name,
        (
            ("gyro_noise_density", gyro),
            ("accel_noise_density", accel),
            ("gyro_bias_sigma", sigma),
            ("gyro_bias_tau", tau),
        ),
        comment,
        delegate=delegate,
    )


def _gps(name, values, comment="", delegate=""):
    names = (
        "horizontal_noise_sigma_m", "altitude_noise_sigma_m", "velocity_noise_sigma_mps",
        "position_drift_sigma", "position_drift_tau", "update_rate_hz", "delay_ms",
        "ttff_cold_s", "ttff_warm_s", "ttff_hot_s", "max_satellites",
    )
    klass = FIX_CLASS_RTK_FIXED if name in RTK_ENTRIES else FIX_CLASS_STANDALONE_3D
    return Variant(name, tuple(zip(names, values)), comment, klass, delegate)


def _single(name, field, literal, comment=""):
    return Variant(name, ((field, literal),), comment)


_IMU_MPU6000 = ("8.726646e-4", "6.37e-3", "1e-4", "100.0")
_GPS_M8N = ("1.5", "3.0", "0.10", "0.06", "30.0", "5.0", "120.0", "26.0", "5.0", "1.0", "22")

_IMU = PartType(
    key="imu", enum="ImuChip", profile="ImuProfile", profile_fn="imu_profile",
    optional=False, parser="parse_imu_chip",
    measured=(
        ("gyro_noise_density", "gyro.gyro_noise_density"),
        ("accel_noise_density", "accel.accel_noise_density"),
    ),
    variants=(
        _imu("Mpu6000", *_IMU_MPU6000,
             comment="0.05 deg/s/sqrt(Hz), 650 ug/sqrt(Hz) - legacy reference, used in tests"),
        _imu("Mpu6500", "9.4e-4", "6.86e-3", "1e-4", "100.0",
             comment="Slightly noisier than MPU6000"),
        _imu("Icm20689", "8.73e-5", "7.85e-4", "5e-5", "200.0",
             comment="0.005 deg/s/sqrt(Hz), 80 ug/sqrt(Hz)"),
        _imu("Icm42688p", "4.88e-5", "6.86e-4", "5e-5", "200.0",
             comment="0.0028 deg/s/sqrt(Hz), 70 ug/sqrt(Hz) - modern flagship, ~17x quieter than MPU6000"),
        _imu("Bmi270", "1.40e-4", "2.45e-3", "8e-5", "150.0",
             comment="0.008 deg/s/sqrt(Hz), 250 ug/sqrt(Hz)"),
        _imu("Lsm6dso", "6.11e-5", "5.88e-4", "6e-5", "180.0",
             comment="0.0035 deg/s/sqrt(Hz), 60 ug/sqrt(Hz)"),
        _imu("Other", *_IMU_MPU6000,
             comment="Unknown chip - fall back to legacy reference",
             delegate="ImuChip::Mpu6000.imu_profile()"),
    ),
    no_profile_variants=(),
    doc=(
        "Datasheet-derived noise profile for this IMU chipset.",
        "",
        "Sources: InvenSense MPU-6000/6500/ICM-20689/ICM-42688P datasheets;",
        "Bosch BMI270; STMicroelectronics LSM6DSO. Bias drift values are",
        "empirical from PX4 EKF2 tuning recommendations.",
    ),
)

_BARO = PartType(
    key="baro", enum="BaroChip", profile="BaroProfile", profile_fn="baro_profile",
    optional=False, parser="parse_baro_chip",
    measured=(("noise_sigma_m", "baro.chip_noise_sigma"),),
    variants=(
        _single("Bmp280", "noise_sigma_m", "0.16"),
        _single("Bmp388", "noise_sigma_m", "0.08"),
        _single("Dps310", "noise_sigma_m", "0.05", comment="best in class"),
        _single("Spl06", "noise_sigma_m", "0.10"),
        _single("Ms5611", "noise_sigma_m", "0.10"),
        _single("Other", "noise_sigma_m", "0.15"),
    ),
    no_profile_variants=(),
    doc=("Datasheet-derived altitude noise standard deviation.",),
)

_MAG = PartType(
    key="mag", enum="MagChip", profile="MagProfile", profile_fn="mag_profile",
    optional=True, parser="parse_mag_chip",
    measured=(("noise_sigma_gauss", "mag.noise_sigma_gauss"),),
    variants=(
        _single("Hmc5883", "noise_sigma_gauss", "0.0006", comment="~0.6 mGauss noise"),
        _single("Qmc5883", "noise_sigma_gauss", "0.0005",
                comment="~0.5 mGauss; cheap, temperature-unstable"),
        _single("Ist8310", "noise_sigma_gauss", "0.0003", comment="~0.3 mGauss"),
        _single("Rm3100", "noise_sigma_gauss", "0.000013",
                comment="~0.013 mGauss - high-performance fluxgate-class"),
        _single("Lis3mdl", "noise_sigma_gauss", "0.00032", comment="~0.32 mGauss"),
        _single("Other", "noise_sigma_gauss", "0.0006"),
    ),
    no_profile_variants=("None",),
    doc=("Datasheet-derived noise profile, or `None` for a build without a mag.",),
)

_GPS = PartType(
    key="gps", enum="GpsChipset", profile="GpsProfile", profile_fn="gps_profile",
    optional=False, parser="parse_gps_chipset",
    measured=(
        ("horizontal_noise_sigma_m", "gps.eph_median_m"),
        ("altitude_noise_sigma_m", "gps.epv_median_m"),
        ("velocity_noise_sigma_mps", "gps.speed_accuracy_median_m_s"),
        ("update_rate_hz", "gps.update_rate_hz"),
    ),
    variants=(
        _gps("UbloxM8N", _GPS_M8N,
             comment="u-blox NEO-M8N: 2.5 m CEP, 18 Hz max (5 Hz default), GPS+GLONASS"),
        _gps("UbloxM9N",
             ("0.9", "1.5", "0.05", "0.04", "40.0", "10.0", "80.0", "24.0", "2.0", "1.0", "33"),
             comment="u-blox NEO-M9N: 1.5 m CEP, 25 Hz max, 4-constellation"),
        _gps("UbloxM10",
             ("1.0", "1.8", "0.06", "0.04", "40.0", "10.0", "90.0", "24.0", "2.0", "1.0", "32"),
             comment="u-blox M10: similar accuracy to M9N, lower power, single-band"),
        _gps("UbloxF9P",
             ("0.014", "0.02", "0.015", "0.005", "60.0", "8.0", "60.0", "25.0", "2.0", "1.0", "60"),
             comment="u-blox ZED-F9P: 0.01 m RTK fixed, 1.5 m standalone, 8 Hz typical"),
        _gps("SeptentrioMosaic",
             ("0.006", "0.012", "0.010", "0.003", "60.0", "100.0", "40.0", "45.0", "20.0", "5.0", "80"),
             comment="Septentrio Mosaic-X5: 0.006 m RTK, 100 Hz, survey-grade"),
        _gps("Here3",
             ("0.9", "1.5", "0.05", "0.04", "40.0", "8.0", "100.0", "24.0", "2.0", "1.0", "33"),
             comment="CubePilot Here3 (CAN GPS): built on u-blox M9N; delay_ms 100.0 is CAN bus latency"),
        _gps("Other", _GPS_M8N,
             comment="Unknown - fall back to M8N legacy reference",
             delegate="GpsChipset::UbloxM8N.gps_profile()"),
    ),
    no_profile_variants=(),
    doc=(
        "Datasheet-derived GPS receiver profile.",
        "",
        "Sources: u-blox M8/M9/M10/F9P product summaries; Septentrio Mosaic-X5",
        "brochure; Here3 (CubePilot) docs. CEP values are typical autonomous",
        "performance - RTK figures are post-base-station-correction (F9P,",
        "Mosaic). TTFF values are from the respective product specs.",
    ),
)

PARTS: Mapping[str, PartType] = MappingProxyType({p.key: p for p in (_IMU, _BARO, _MAG, _GPS)})


def normalize(text: str) -> str:
    """The daemon's rule: lowercase, then remove `-`, `_` and spaces."""
    return text.lower().replace("-", "").replace("_", "").replace(" ", "")


def daemon_parse(part_type: str, text: str) -> str:
    """The 0.27.0 parser's result: a variant name, "Other", or "None" for mag."""
    normalized = normalize(text)
    if part_type == "gps":
        for needles, name in _GPS_RULES:
            if any(needle in normalized for needle in needles):
                return name
        return OTHER
    return _ALIASES[part_type].get(normalized, OTHER)


def variant(part_type: str, name: str) -> Variant | None:
    """The existing variant of that name (Other included), else None."""
    return next((v for v in PARTS[part_type].variants if v.name == name), None)


def candidate_variant_name(log_name: str) -> str:
    """Split on every run of characters other than ASCII letters and digits, capitalize each
    piece: "EMLID_REACH" -> "EmlidReach", "EMLID-REACH M2" -> "EmlidReachM2".

    The result is not always a usable Rust variant ("1ABC" -> "1abc", "OTHER" -> "Other");
    the Rust emitter checks every name it writes and refuses those."""
    pieces = _NAME_SEPARATORS.split(log_name)
    return "".join(piece[:1].upper() + piece[1:].lower() for piece in pieces)


def classify_log_part(part_type: str, log_name: str | None) -> Classified:
    """Where a part name from a log belongs in the part table.

    "excluded" is a simulated device; "other_only" is an unidentified or
    generic name that counts only toward Other; "existing" maps to a table
    variant; "candidate" is a real part with no entry yet.
    """
    log_name = (log_name or "").strip()
    if not log_name:
        return Classified(KIND_OTHER_ONLY, OTHER)
    if log_name.startswith(UNKNOWN_PREFIX) or log_name in GENERIC_LOG_NAMES[part_type]:
        return Classified(KIND_OTHER_ONLY, OTHER)
    if log_name == SIMULATED_LOG_NAMES[part_type]:
        return Classified(KIND_EXCLUDED, "")
    mapped = LOG_NAME_VARIANTS.get(part_type, {}).get(log_name)
    if mapped is not None:
        return Classified(KIND_EXISTING, mapped)
    parsed = daemon_parse(part_type, log_name)
    if parsed in PARTS[part_type].no_profile_variants:
        return Classified(KIND_OTHER_ONLY, OTHER)
    if parsed != OTHER and variant(part_type, parsed) is not None:
        return Classified(KIND_EXISTING, parsed)
    return Classified(KIND_CANDIDATE, candidate_variant_name(log_name))


def current_literal(part_type: str, entry: str, field: str) -> str:
    """The 0.13.1 literal; any entry that is not an existing variant gets Other's.

    ValueError for a variant without a profile (MagChip::None): it has no value."""
    if entry in PARTS[part_type].no_profile_variants:
        raise ValueError(f"{part_type}: {entry} has no profile, so no current value")
    found = variant(part_type, entry) or variant(part_type, OTHER)
    return dict(found.fields)[field]


def current_value(part_type: str, entry: str, field: str) -> float:
    return float(current_literal(part_type, entry, field))


def fix_class(entry: str) -> str:
    """RTK fixed for the RTK receivers, standalone 3D for everything else."""
    return FIX_CLASS_RTK_FIXED if entry in RTK_ENTRIES else FIX_CLASS_STANDALONE_3D
