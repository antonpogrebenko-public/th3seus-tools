"""Extract step of the battery fit: one PX4 log -> LogBatteryData -> <log_id>.npz.

Each selected log is read once (battery_status only) and leaves exactly one
file: <log_id>.npz when it is a usable real log, else an empty
<log_id>.<status> file, so the fit step (battery_fit) still places that log
in the split.  Simulation logs are detected via ``hardware.is_simulation()``.
"""

from __future__ import annotations

import hashlib
import os
import sys
import zipfile
import zlib
from collections import Counter
from collections.abc import Callable, Collection, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, TypeVar

import numpy as np
from pyulog import ULog

from .hardware import extract_hardware_info, is_simulation
from .manifest import DEFAULT_MAX_PER_VEHICLE, select_logs, selection_counts
from .parallel import run_per_log
from .ulog_loader import US_TO_S

T = TypeVar("T")

# ---------------------------------------------------------------------------
# Extract step constants (the values of the one-pass analysis, named)
# ---------------------------------------------------------------------------

# The only topic the extract step parses.  The log header (msg_info_dict and
# initial_parameters, which extract_hardware_info needs) is read regardless.
BATTERY_TOPIC: str = "battery_status"

# Pack checks: a usable log has 1 to MAX_CELL_COUNT cells, a voltage that moves
# by at least MIN_VOLTAGE_RANGE_V and a peak current of at least MIN_PEAK_CURRENT_A.
MAX_CELL_COUNT: int = 14
MIN_VOLTAGE_RANGE_V: float = 0.1
MIN_PEAK_CURRENT_A: float = 2.0

# Status of each selected log after the extract step.  An "ok" log is saved as
# <log_id>.npz; any other status is saved as an empty <log_id>.<status> file,
# so the fit step still places that log in the split (see fit_extracted).
# "unusable" logs loaded but lack usable battery data; a "load_error" or
# "worker_died" log was never read, so the fit does not count it as real.
STATUS_OK = "ok"
STATUS_SIM = "sim"
STATUS_UNUSABLE = "unusable"
STATUS_LOAD_ERROR = "load_error"
STATUS_WORKER_DIED = "worker_died"
STATUSES = (STATUS_OK, STATUS_SIM, STATUS_UNUSABLE, STATUS_LOAD_ERROR, STATUS_WORKER_DIED)
# The statuses a selected log without extracted data can have (fit_extracted's no_data).
NO_DATA_STATUSES = frozenset({STATUS_SIM, STATUS_UNUSABLE, STATUS_LOAD_ERROR, STATUS_WORKER_DIED})
# Not a selected log: an empty <log_id>.no_manifest_row marks a log of log_dir
# that the manifest leaves out, so the fit can count it (and never fit it).
STATUS_NO_MANIFEST_ROW = "no_manifest_row"
NPZ_SUFFIX = ".npz"
# A finished extract_dir run writes extract-<digest>.done listing its log IDs.
DONE_SUFFIX = ".done"
_SUFFIX_OF_STATUS = {
    STATUS_OK: NPZ_SUFFIX,
    **{s: f".{s}" for s in (*STATUSES, STATUS_NO_MANIFEST_ROW) if s != STATUS_OK},
}
_STATUS_OF_SUFFIX = {suffix: status for status, suffix in _SUFFIX_OF_STATUS.items()}
_REQUIRED_KEYS = ("log_id", "cell_count", "t_s", "v", "i")
_OPTIONAL_KEYS = ("remaining", "discharged_mah")


# ---------------------------------------------------------------------------
# Public types
# ---------------------------------------------------------------------------


@dataclass(frozen=True, eq=False)
class LogBatteryData:
    """battery_status series of one usable real log, as the fit step reads it.

    eq=False: a field-wise == over numpy arrays is ambiguous; compare the
    arrays with numpy instead.
    """

    log_id: str
    cell_count: int
    t_s: np.ndarray
    v: np.ndarray
    i: np.ndarray
    remaining: np.ndarray | None
    discharged_mah: np.ndarray | None


# ---------------------------------------------------------------------------
# Extract step: one log -> LogBatteryData -> <log_id>.npz
# ---------------------------------------------------------------------------


def load_battery_topics(path: Path) -> ULog:
    """Parse only battery_status; the log header extract_hardware_info needs is always read."""
    return ULog(str(path), message_name_filter_list=[BATTERY_TOPIC])


def extract_log(
    path: Path, loader: Callable[[Path], Any] = load_battery_topics
) -> tuple[str, LogBatteryData | None]:
    """Read one log: ("ok", data), or ("sim" | "unusable" | "load_error", None).

    Load error: the loader raised; "<log_id>: <exception type>" goes to stderr
    (not the message, which often holds the file path).  Unusable: no
    battery_status, or the pack fails the checks of the one-pass analysis
    (cell count, voltage range, current).
    """
    try:
        ulog = loader(path)
    except Exception as e:
        print(f"{Path(path).stem}: {type(e).__name__}", file=sys.stderr)
        return STATUS_LOAD_ERROR, None
    if _is_simulation_log(ulog):
        return STATUS_SIM, None
    data = _battery_data(Path(path).stem, ulog)
    return (STATUS_UNUSABLE, None) if data is None else (STATUS_OK, data)


def _is_simulation_log(ulog: Any) -> bool:
    """SITL/HIL check; a log whose header cannot be read counts as real, as before."""
    try:
        hw = extract_hardware_info(ulog.msg_info_dict, ulog.initial_parameters)
    except Exception:
        return False
    return is_simulation(hw)


def _battery_data(log_id: str, ulog: Any) -> LogBatteryData | None:
    if BATTERY_TOPIC not in {d.name for d in ulog.data_list}:
        return None
    try:
        ds = ulog.get_dataset(BATTERY_TOPIC, multi_instance=0)
        v = np.asarray(ds.data["voltage_v"], dtype=float)
        i = np.asarray(ds.data["current_a"], dtype=float)
        t_s = np.asarray(ds.data["timestamp"], dtype=float) * US_TO_S
        cells = int(ds.data["cell_count"][0])
    except (KeyError, IndexError, ValueError):
        return None
    if not _pack_is_usable(v, i, cells):
        return None
    return LogBatteryData(
        log_id=log_id,
        cell_count=cells,
        t_s=t_s,
        v=v,
        i=i,
        remaining=_optional_series(ds.data, "remaining"),
        discharged_mah=_optional_series(ds.data, "discharged_mah"),
    )


def _pack_is_usable(v: np.ndarray, i: np.ndarray, cells: int) -> bool:
    """The one-pass analysis's checks, kept as rejections so a NaN passes as it did."""
    if cells <= 0 or cells > MAX_CELL_COUNT:
        return False
    if v.max() - v.min() < MIN_VOLTAGE_RANGE_V:
        return False
    if i.max() < MIN_PEAK_CURRENT_A:
        return False
    return True


def _optional_series(data: Mapping[str, Any], name: str) -> np.ndarray | None:
    return np.asarray(data[name], dtype=float) if name in data else None


def save_extracted(data: LogBatteryData, out_dir: Path) -> Path:
    """Write <out_dir>/<log_id>.npz; an interrupted write never takes that name."""
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    path = out / f"{data.log_id}{NPZ_SUFFIX}"
    partial = out / f"{data.log_id}{NPZ_SUFFIX}.part"
    arrays = {
        "log_id": np.array(data.log_id),
        "cell_count": np.array(data.cell_count),
        "t_s": data.t_s,
        "v": data.v,
        "i": data.i,
    }
    optional = {k: getattr(data, k) for k in _OPTIONAL_KEYS if getattr(data, k) is not None}
    with open(partial, "wb") as fh:
        np.savez_compressed(fh, **arrays, **optional)
    os.replace(partial, path)
    return path


def load_extracted(path: Path) -> LogBatteryData:
    """Read a file written by save_extracted; ValueError if it is malformed."""
    name = Path(path).name
    arrays = _read_npz(Path(path))
    missing = [k for k in _REQUIRED_KEYS if k not in arrays]
    if missing:
        raise ValueError(f"{name}: missing {', '.join(missing)}")
    data = LogBatteryData(
        log_id=_scalar(arrays, "log_id", name, str),
        cell_count=_scalar(arrays, "cell_count", name, int),
        t_s=arrays["t_s"],
        v=arrays["v"],
        i=arrays["i"],
        remaining=arrays.get("remaining"),
        discharged_mah=arrays.get("discharged_mah"),
    )
    series = [data.t_s, data.v, data.i, data.remaining, data.discharged_mah]
    shapes = {a.shape for a in series if a is not None}
    if len(shapes) != 1 or len(next(iter(shapes))) != 1:
        raise ValueError(f"{name}: series must be one-dimensional and of one length")
    if not 1 <= data.cell_count <= MAX_CELL_COUNT:
        raise ValueError(f"{name}: cell_count {data.cell_count} is outside 1..{MAX_CELL_COUNT}")
    return data


def _scalar(arrays: dict[str, np.ndarray], key: str, name: str, convert: Callable[[Any], T]) -> T:
    """arrays[key] converted, or ValueError when it is not a single convertible value."""
    value = arrays[key]
    if value.ndim != 0:
        raise ValueError(f"{name}: {key} must be a single value, got shape {value.shape}")
    try:
        return convert(value)
    except (TypeError, ValueError) as e:
        raise ValueError(f"{name}: {key} is not a valid value ({type(e).__name__})") from e


def _read_npz(path: Path) -> dict[str, np.ndarray]:
    """Every array of an .npz file; a file numpy cannot read as one is a ValueError."""
    try:
        with np.load(path, allow_pickle=False) as npz:
            return {k: npz[k] for k in npz.files}
    except (ValueError, EOFError, AttributeError, TypeError, zipfile.BadZipFile, zlib.error) as e:
        # AttributeError (Python 3.10) or TypeError (3.11 and later): numpy returned a
        # plain array (an .npy), which is not a context manager and has no `files`.
        raise ValueError(f"{path.name}: not a readable extract file ({type(e).__name__})") from e


def extract_dir(
    log_dir: Path,
    out_dir: Path,
    manifest_path: Path | None = None,
    max_per_vehicle: int = DEFAULT_MAX_PER_VEHICLE,
    jobs: int = 1,
    loader: Callable[[Path], Any] = load_battery_topics,
) -> dict[str, int]:
    """Extract the selected logs of log_dir into out_dir; return the count per status.

    The selection is manifest.select_logs (manifest rows, per-vehicle cap).
    Each selected log leaves exactly one file in out_dir: <log_id>.npz when
    usable, else an empty <log_id>.<status>.  With jobs > 1 the logs run in
    worker processes (parallel.run_per_log); a log whose worker dies gets the
    status "worker_died" and the other logs are still extracted; loader must
    then be a top-level function so it pickles.  A log the manifest leaves out
    gets an empty <log_id>.no_manifest_row marker, so the fit counts it.  Only
    a run that finishes writes extract-<digest>.done listing its logs; the fit
    refuses logs no done file lists.  The counts also hold the selection's
    manifest_rows_without_log, logs_without_manifest_row and capped_out
    (manifest.selection_counts).
    """
    selection = select_logs(Path(log_dir), manifest_path, max_per_vehicle)
    if not selection.paths:
        raise FileNotFoundError(f"No .ulg files selected in {str(log_dir)!r}")
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    done = _done_file(out, [p.stem for p in selection.paths] + list(selection.unlisted_ids))
    done.unlink(missing_ok=True)  # a rerun that stops midway must not look finished
    tasks = [(p, out, loader) for p in selection.paths]
    statuses = run_per_log(_extract_task, tasks, jobs, _extract_died)
    for path, status in zip(selection.paths, statuses):
        _record_status(out, path.stem, status)
    for log_id in selection.unlisted_ids:
        _record_status(out, log_id, STATUS_NO_MANIFEST_ROW)
    _write_done(done, [p.stem for p in selection.paths] + list(selection.unlisted_ids))
    return {**{s: statuses.count(s) for s in STATUSES}, **selection_counts(selection)}


def _extract_task(task: tuple[Path, Path, Callable[[Path], Any]]) -> str:
    """Worker: extract one log, save it when usable, return its status."""
    path, out_dir, loader = task
    status, data = extract_log(path, loader)
    if data is not None:
        save_extracted(data, out_dir)
    return status


def _extract_died(task: tuple[Path, Path, Callable[[Path], Any]]) -> str:
    return STATUS_WORKER_DIED


def _record_status(out_dir: Path, log_id: str, status: str) -> None:
    """Leave one file for log_id: drop the files of its other statuses, mark a non-ok one."""
    for other, suffix in _SUFFIX_OF_STATUS.items():
        if other != status:
            (out_dir / f"{log_id}{suffix}").unlink(missing_ok=True)
    if status != STATUS_OK:
        (out_dir / f"{log_id}{_SUFFIX_OF_STATUS[status]}").touch()


def read_statuses(obs_dir: Path) -> dict[str, str]:
    """{log_id: status} from the files extract_dir left; other files are ignored."""
    found = sorted(
        (p.stem, _STATUS_OF_SUFFIX[p.suffix])
        for p in Path(obs_dir).iterdir()
        if p.is_file() and p.suffix in _STATUS_OF_SUFFIX
    )
    repeated = sorted(i for i, n in Counter(i for i, _ in found).items() if n > 1)
    if repeated:
        raise ValueError(f"log ID with more than one extracted file: {', '.join(repeated[:5])}")
    return dict(found)


def _done_file(out_dir: Path, log_ids: Sequence[str]) -> Path:
    """The file a finished extract_dir run leaves, named after the logs it covers."""
    digest = hashlib.sha256("\n".join(sorted(log_ids)).encode("utf-8")).hexdigest()[:16]
    return out_dir / f"extract-{digest}{DONE_SUFFIX}"


def _write_done(done: Path, log_ids: Sequence[str]) -> None:
    partial = done.with_name(done.name + ".part")
    partial.write_text("".join(f"{i}\n" for i in sorted(log_ids)), encoding="utf-8")
    os.replace(partial, done)


def _check_finished(obs_dir: Path, log_ids: Collection[str]) -> None:
    """Refuse a folder whose log files no finished extract_dir run lists, or the reverse.

    Each run lists its logs in its own done file, so batches merged into one
    folder are checked run by run: an interrupted batch is caught even when
    another batch in the folder finished.
    """
    listed = {
        line for f in Path(obs_dir).glob(f"*{DONE_SUFFIX}")
        for line in f.read_text(encoding="utf-8").split()
    }
    unfinished = sorted(set(log_ids) - listed)
    if unfinished:
        raise ValueError(
            f"{obs_dir}: {len(unfinished)} extracted log(s) belong to an interrupted extract "
            f"run, e.g. {unfinished[0]}; run --extract-to again for that batch"
        )
    missing = sorted(listed - set(log_ids))
    if missing:
        raise ValueError(f"{obs_dir}: {len(missing)} log(s) of a finished extract run have "
                         f"no file, e.g. {missing[0]}; the folder is incomplete")


def read_obs_dirs(obs_dirs: Sequence[Path]) -> dict[str, tuple[str, Path]]:
    """{log_id: (status, folder)} over one or more finished extract_dir folders.

    A log ID found in more than one file, in one folder or across folders, is
    refused, and so is a folder holding logs of an interrupted run.
    """
    per_dir = [(Path(d), read_statuses(Path(d))) for d in obs_dirs]
    for folder, statuses in per_dir:
        _check_finished(folder, statuses)
    found = [(i, s, d) for d, statuses in per_dir for i, s in statuses.items()]
    repeated = sorted(i for i, n in Counter(i for i, _, _ in found).items() if n > 1)
    if repeated:
        raise ValueError(f"log ID with more than one extracted file: {', '.join(repeated[:5])}")
    return {i: (s, d) for i, s, d in found}


def load_named(obs_dir: Path, log_id: str) -> LogBatteryData:
    data = load_extracted(obs_dir / f"{log_id}{NPZ_SUFFIX}")
    if data.log_id != log_id:
        raise ValueError(f"{log_id}{NPZ_SUFFIX} holds log {data.log_id}")
    return data
