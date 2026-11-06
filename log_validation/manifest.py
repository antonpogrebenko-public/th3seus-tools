"""Log manifest (log ID -> vehicle ID) and per-vehicle log selection."""

from __future__ import annotations

import argparse
import csv
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from itertools import groupby, islice
from pathlib import Path

LOG_ID_COLUMN = "log_id"
VEHICLE_ID_COLUMN = "vehicle_id"
DEFAULT_MAX_PER_VEHICLE = 3


class ManifestError(ValueError):
    """The manifest file is malformed."""


@dataclass(frozen=True)
class Selection:
    paths: tuple[Path, ...]
    vehicle_of: dict[str, str] = field(default_factory=dict)
    manifest_rows_without_log: int = 0
    logs_without_manifest_row: int = 0
    capped_out: int = 0
    unlisted_ids: tuple[str, ...] = ()  # the logs counted in logs_without_manifest_row


def read_manifest(path: Path) -> dict[str, str]:
    """Read a CSV with columns log_id and vehicle_id into {log_id: vehicle_id}."""
    mapping: dict[str, str] = {}
    with open(path, newline="") as fh:
        reader = csv.DictReader(fh)
        names = [(n or "").strip() for n in (reader.fieldnames or [])]
        missing = [c for c in (LOG_ID_COLUMN, VEHICLE_ID_COLUMN) if c not in names]
        if missing:
            raise ManifestError(f"manifest is missing column(s): {', '.join(missing)}")
        for row in reader:
            if None in row:  # csv puts the fields beyond the header under the key None
                raise ManifestError(
                    f"manifest line {reader.line_num} has more fields than the header")
            clean = {(k or "").strip(): (v or "").strip() for k, v in row.items()}
            log_id, vehicle = clean[LOG_ID_COLUMN], clean[VEHICLE_ID_COLUMN]
            if not log_id and not vehicle:
                continue
            if not log_id or not vehicle:
                raise ManifestError(f"manifest row has an empty field: {log_id or vehicle}")
            if log_id in mapping:
                raise ManifestError(f"log ID repeated in manifest: {log_id}")
            mapping[log_id] = vehicle
    return mapping


def select_logs(
    log_dir: Path,
    manifest_path: Path | None = None,
    max_per_vehicle: int = DEFAULT_MAX_PER_VEHICLE,
) -> Selection:
    """List the .ulg files of log_dir, restricted by the manifest and the per-vehicle cap."""
    check_max_per_vehicle(max_per_vehicle)
    found = sorted(Path(log_dir).glob("*.ulg"), key=lambda p: p.stem)
    if manifest_path is None:
        return Selection(paths=tuple(found))

    vehicle_of = read_manifest(manifest_path)
    stems = {p.stem for p in found}
    rows_without_log = sum(1 for log_id in vehicle_of if log_id not in stems)
    listed = [p for p in found if p.stem in vehicle_of]
    kept_ids = set(cap_per_vehicle([p.stem for p in listed], vehicle_of, max_per_vehicle))
    kept = [p for p in listed if p.stem in kept_ids]
    unlisted = tuple(p.stem for p in found if p.stem not in vehicle_of)
    return Selection(
        paths=tuple(kept),
        vehicle_of={p.stem: vehicle_of[p.stem] for p in kept},
        manifest_rows_without_log=rows_without_log,
        logs_without_manifest_row=len(unlisted),
        capped_out=len(listed) - len(kept),
        unlisted_ids=unlisted,
    )


def selection_counts(selection: Selection) -> dict[str, int]:
    """The selection's input counts, in a fixed order, for a pass to report."""
    return {
        "manifest_rows_without_log": selection.manifest_rows_without_log,
        "logs_without_manifest_row": selection.logs_without_manifest_row,
        "capped_out": selection.capped_out,
    }


def format_counts(counts: Mapping[str, int]) -> str:
    """Counts on one line, "name: N | name: N", as the passes print them to stderr."""
    return " | ".join(f"{name}: {n}" for name, n in counts.items())


def check_max_per_vehicle(max_per_vehicle: int) -> None:
    """Refuse a negative per-vehicle cap (0 keeps no log)."""
    if max_per_vehicle < 0:
        raise ValueError(f"max_per_vehicle must be 0 or more, got {max_per_vehicle}")


def cap_per_vehicle(
    log_ids: Iterable[str], vehicle_of: Mapping[str, str], max_per_vehicle: int
) -> list[str]:
    """The max_per_vehicle smallest log IDs of each vehicle, sorted; every ID needs a vehicle.

    The one rule for the cap: select_logs (.ulg files) and the battery fit over
    extracted files both use it, so a one-pass and a two-step run keep the same logs.
    """
    check_max_per_vehicle(max_per_vehicle)
    by_vehicle = sorted(log_ids, key=lambda i: (vehicle_of[i], i))
    return sorted(
        i for _, group in groupby(by_vehicle, key=lambda i: vehicle_of[i])
        for i in islice(group, max_per_vehicle)
    )


def _cap_arg(text: str) -> int:
    value = int(text)
    if value < 0:
        raise argparse.ArgumentTypeError(f"must be 0 or more, got {value}")
    return value


def add_selection_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--manifest", type=Path, default=None, metavar="PATH",
                        help="CSV with columns log_id,vehicle_id; logs not listed are skipped")
    parser.add_argument("--max-per-vehicle", type=_cap_arg, default=DEFAULT_MAX_PER_VEHICLE,
                        metavar="N", help="keep the N smallest log IDs of each vehicle (default 3)")
    parser.add_argument("--jobs", type=int, default=1, metavar="N",
                        help="parallel worker processes (default 1)")
