"""Fake ULog with multi-instance topics, for the part identification tests."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import numpy as np

from .fakes import ts_us


class _Instance:
    def __init__(self, name: str, multi_id: int, data: Mapping[str, Any]):
        self.name = name
        self.multi_id = multi_id
        self.data = dict(data)


class InstanceULog:
    """Like fakes.FakeULog, but a key (topic, i) is instance i; a plain name is instance 0."""

    def __init__(
        self,
        datasets: Mapping[str | tuple[str, int], Mapping[str, Any]],
        params: Mapping[str, Any] | None = None,
        info: Mapping[str, str] | None = None,
    ):
        self.data_list = []
        for key, data in datasets.items():
            name, multi_id = (key, 0) if isinstance(key, str) else key
            self.data_list.append(_Instance(name, multi_id, data))
        self.initial_parameters = dict(params or {})
        self.msg_info_dict = dict(info or {})

    def get_dataset(self, name, multi_instance=0):
        for d in self.data_list:
            if d.name == name and d.multi_id == multi_instance:
                return d
        raise KeyError(name)


def device_id(devtype: int, bus_type: int = 2, bus: int = 1, address: int = 0) -> int:
    """A PX4 packed device ID: bus_type 3 bits, bus 5 bits, address 8 bits, devtype 8 bits."""
    return (devtype << 16) | (address << 8) | (bus << 3) | bus_type


def topic(n: int, rate_hz: float, **fields: Any) -> dict[str, np.ndarray]:
    """n samples at rate_hz: a scalar field is repeated n times, a sequence is used as given."""
    data = {"timestamp": ts_us(n, rate_hz)}
    for name, value in fields.items():
        data[name] = np.full(n, value) if np.isscalar(value) else np.asarray(value)
    return data
