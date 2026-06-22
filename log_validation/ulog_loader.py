"""Thin pyulog wrapper: load a .ulg and pull named series as numpy arrays.

Field names are not hardcoded blindly. `series()` raises a clear error listing
the available fields when a name is missing, so a topic/field rename in PX4
surfaces immediately instead of silently producing wrong numbers.
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
from pyulog import ULog

# PX4 stores timestamps in microseconds (since boot, not calendar time).
US_TO_S = 1e-6
# vehicle_gps_position scaling: lat/lon are int 1e-7 deg, alt is int mm.
GPS_LATLON_SCALE = 1e-7
GPS_ALT_SCALE = 1e-3


@dataclass(frozen=True)
class Series:
    """A time series for one field of one topic instance."""

    t_s: np.ndarray
    values: np.ndarray


def load(path: str) -> ULog:
    """Load a ulog file."""
    return ULog(path)


def topic_names(ulog: ULog) -> dict[str, int]:
    """Map of available topic name -> sample count (first instance shown)."""
    out: dict[str, int] = {}
    for d in ulog.data_list:
        out.setdefault(d.name, len(d.data["timestamp"]))
    return out


def _dataset(ulog: ULog, topic: str, instance: int):
    try:
        return ulog.get_dataset(topic, multi_instance=instance)
    except Exception as exc:  # pyulog raises bare Exception when missing
        available = sorted({d.name for d in ulog.data_list})
        raise KeyError(
            f"topic '{topic}' (instance {instance}) not in log. "
            f"Available topics: {available}"
        ) from exc


def series(ulog: ULog, topic: str, field: str, instance: int = 0) -> Series:
    """Return (t_s, values) for one field, timestamps converted to seconds."""
    ds = _dataset(ulog, topic, instance)
    if field not in ds.data:
        raise KeyError(
            f"field '{field}' not in topic '{topic}'. "
            f"Available fields: {sorted(ds.data.keys())}"
        )
    t_s = np.asarray(ds.data["timestamp"], dtype=float) * US_TO_S
    values = np.asarray(ds.data[field], dtype=float)
    return Series(t_s=t_s, values=values)


def xyz(ulog: ULog, topic: str, instance: int = 0) -> tuple[np.ndarray, np.ndarray]:
    """Return (t_s, Nx3 array) for a topic exposing x/y/z fields."""
    ds = _dataset(ulog, topic, instance)
    for f in ("x", "y", "z"):
        if f not in ds.data:
            raise KeyError(
                f"topic '{topic}' missing field '{f}'. "
                f"Available fields: {sorted(ds.data.keys())}"
            )
    t_s = np.asarray(ds.data["timestamp"], dtype=float) * US_TO_S
    arr = np.column_stack(
        [
            np.asarray(ds.data["x"], float),
            np.asarray(ds.data["y"], float),
            np.asarray(ds.data["z"], float),
        ]
    )
    return t_s, arr
