"""Log manifest (log ID -> vehicle ID) and per-vehicle log selection."""

from __future__ import annotations

import argparse
import csv
from dataclasses import dataclass, field
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
    found = sorted(Path(log_dir).glob("*.ulg"), key=lambda p: p.stem)
    if manifest_path is None:
        return Selection(paths=tuple(found))

    vehicle_of = read_manifest(manifest_path)
    stems = {p.stem for p in found}
    rows_without_log = sum(1 for log_id in vehicle_of if log_id not in stems)
    listed = [p for p in found if p.stem in vehicle_of]

    kept: list[Path] = []
    seen: dict[str, int] = {}
    for p in listed:  # already sorted by stem: the smallest IDs of each vehicle win
        vehicle = vehicle_of[p.stem]
        if seen.get(vehicle, 0) < max_per_vehicle:
            seen = {**seen, vehicle: seen.get(vehicle, 0) + 1}
            kept.append(p)
    return Selection(
        paths=tuple(kept),
        vehicle_of={p.stem: vehicle_of[p.stem] for p in kept},
        manifest_rows_without_log=rows_without_log,
        logs_without_manifest_row=len(found) - len(listed),
        capped_out=len(listed) - len(kept),
    )


def add_selection_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--manifest", type=Path, default=None, metavar="PATH",
                        help="CSV with columns log_id,vehicle_id; logs not listed are skipped")
    parser.add_argument("--max-per-vehicle", type=int, default=DEFAULT_MAX_PER_VEHICLE,
                        metavar="N", help="keep the N smallest log IDs of each vehicle (default 3)")
    parser.add_argument("--jobs", type=int, default=1, metavar="N",
                        help="parallel worker processes (default 1)")
