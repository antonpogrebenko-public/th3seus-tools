"""Shared duck-typed fake ULog for tests (matches the pyulog surface log_validation uses)."""

from __future__ import annotations

import numpy as np


class _Dataset:
    def __init__(self, data: dict[str, np.ndarray]):
        self.data = data


class _NamedData:
    def __init__(self, name: str, data: dict[str, np.ndarray]):
        self.name = name
        self.data = data


class FakeULog:
    def __init__(
        self,
        datasets: dict[str, dict[str, np.ndarray]],
        params: dict[str, float] | None = None,
        info: dict[str, str] | None = None,
    ):
        self._datasets = {name: _Dataset(d) for name, d in datasets.items()}
        self.data_list = [_NamedData(name, d) for name, d in datasets.items()]
        self.initial_parameters = dict(params or {})
        self.msg_info_dict = dict(info or {})

    def get_dataset(self, name, multi_instance=0):
        return self._datasets[name]  # raises KeyError if absent (loader wraps it)


def ts_us(n: int, rate_hz: float, start_s: float = 0.0) -> np.ndarray:
    """n timestamps in microseconds at rate_hz, starting at start_s seconds."""
    dt_us = 1e6 / rate_hz
    return start_s * 1e6 + (np.arange(n) * dt_us).astype(np.float64)
