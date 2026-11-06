"""Fake battery logs shared by the battery extract and fit tests.

Top-level functions so a spawned extraction worker can import them.
"""

from __future__ import annotations

import os
import zlib
from pathlib import Path

import numpy as np

from log_validation.battery_extract import LogBatteryData, extract_log
from log_validation.tests.fakes import FakeULog, ts_us

REAL_INFO = {"ver_hw": "PX4_FMU_V5"}
SITL_INFO = {"ver_hw": "PX4_SITL"}
R_4S_MOHM = 9.0
R_3S_MOHM = 11.0
FAKE_RATE_HZ = 5.0
FAKE_SAMPLES = 400

# 16 usable real logs (4S and 3S), 2 simulation logs, and 3 real logs without
# usable battery data (load error, no battery topic, current below 2 A).
CORPUS = (
    [f"a{k:02d}" for k in range(10)]
    + [f"c3-{k:02d}" for k in range(6)]
    + ["sim01", "sim02", "bad01", "nobat01", "flat01"]
)


def battery_status(cells: int, r_mohm: float, seed: int) -> dict[str, np.ndarray]:
    """battery_status of a pack with a linear OCV and r_mohm of resistance per cell."""
    rng = np.random.default_rng(seed)
    soc = np.linspace(1.0, 0.3, FAKE_SAMPLES)
    levels = rng.choice([1.0, 12.0, 25.0, 40.0], size=FAKE_SAMPLES // 10 + 1)
    current = np.repeat(levels, 10)[:FAKE_SAMPLES]
    volts = cells * (3.5 + 0.7 * soc) - current * cells * r_mohm / 1000.0
    return {
        "timestamp": ts_us(FAKE_SAMPLES, FAKE_RATE_HZ),
        "voltage_v": volts + rng.normal(0.0, 0.005, FAKE_SAMPLES),
        "current_a": current,
        "cell_count": np.full(FAKE_SAMPLES, cells),
        "remaining": soc,
    }


def load_by_stem(path: Path) -> FakeULog:
    """Top-level so a spawned worker can import it; the stem's prefix picks the kind of log."""
    stem = path.stem
    if stem.startswith("bad"):
        raise OSError("corrupt log")
    if stem.startswith("die"):
        os._exit(1)
    if stem.startswith("nobat"):
        return FakeULog({"vehicle_status": {"timestamp": ts_us(10, 1.0)}}, info=REAL_INFO)
    cells, r_mohm = (3, R_3S_MOHM) if stem.startswith("c3") else (4, R_4S_MOHM)
    data = battery_status(cells, r_mohm, zlib.crc32(stem.encode()))
    if stem.startswith("flat"):
        data = {**data, "current_a": np.full(FAKE_SAMPLES, 1.0)}
    info = SITL_INFO if stem.startswith("sim") else REAL_INFO
    return FakeULog({"battery_status": data}, info=info)


def fake_from(battery: dict[str, np.ndarray]):
    return lambda _path: FakeULog({"battery_status": battery}, info=REAL_INFO)


def make_data(log_id: str, cells: int = 4, r_mohm: float = R_4S_MOHM) -> LogBatteryData:
    battery = battery_status(cells, r_mohm, zlib.crc32(log_id.encode()))
    status, data = extract_log(Path(f"{log_id}.ulg"), loader=fake_from(battery))
    assert status == "ok" and data is not None
    return data


def make_log_dir(tmp_path: Path, stems) -> Path:
    log_dir = tmp_path / "logs"
    log_dir.mkdir()
    for s in stems:
        (log_dir / f"{s}.ulg").write_bytes(b"")
    return log_dir


def make_manifest(tmp_path: Path, rows: dict[str, str]) -> Path:
    path = tmp_path / "manifest.csv"
    path.write_text("log_id,vehicle_id\n" + "".join(f"{k},{v}\n" for k, v in rows.items()))
    return path


def file_names(directory: Path) -> list[str]:
    """The per-log files of an extract folder (the run's .done file left out)."""
    return sorted(p.name for p in directory.iterdir() if p.suffix != ".done")
