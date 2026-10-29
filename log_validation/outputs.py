"""Guarded, deterministic output writing: no private strings reach a written file."""

from __future__ import annotations

import argparse
import getpass
import json
import math
import os
import socket
import sys
from pathlib import Path
from typing import Any, Mapping, Sequence

from .manifest import DEFAULT_MAX_PER_VEHICLE, cap_per_vehicle, read_manifest

MIN_FORBIDDEN_LEN = 3


class PrivateDataError(ValueError):
    """An output would contain a path, host name or user name."""


def forbidden_strings(*extra: str | Path) -> tuple[str, ...]:
    """Strings no output may contain: home dir, user, host, and each extra (resolved).

    An extra is also forbidden as given when it holds a path separator; a bare
    relative name such as "obs" is not, since it would match ordinary words.
    """
    candidates: list[str] = [str(Path.home()), getpass.getuser(), socket.gethostname()]
    for item in extra:
        given = str(item)
        if os.sep in given or "/" in given:
            candidates.append(given)
        candidates.append(str(Path(item).resolve()))
    out: list[str] = []
    for s in candidates:
        if len(s) >= MIN_FORBIDDEN_LEN and s not in out:
            out.append(s)
    return tuple(out)


def _clean(obj: Any) -> Any:
    """Copy of obj with NaN/inf as None and numpy values as plain Python."""
    if hasattr(obj, "tolist") and not isinstance(obj, (str, bytes)):
        obj = obj.tolist()
    if isinstance(obj, dict):
        return {str(k): _clean(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_clean(v) for v in obj]
    if isinstance(obj, float):
        return obj if math.isfinite(obj) else None
    return obj


def _check(text: str, forbidden: Sequence[str]) -> None:
    for s in forbidden:
        if not s:
            continue
        # Also match the JSON-escaped forms (backslashes, \\uXXXX) of the string.
        variants = {s, json.dumps(s)[1:-1], json.dumps(s, ensure_ascii=False)[1:-1]}
        if any(v in text for v in variants):
            raise PrivateDataError("output contains private data; refusing to write it")


def write_text(path: Path, text: str, forbidden: Sequence[str]) -> None:
    _check(text, forbidden)
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def write_json(path: Path, obj: Any, forbidden: Sequence[str]) -> None:
    text = json.dumps(_clean(obj), indent=1, sort_keys=True, allow_nan=False,
                      ensure_ascii=False) + "\n"
    write_text(path, text, forbidden)


def merge_per_log(paths: Sequence[Path]) -> list[dict]:
    """Merge JSON lists of per-log records, sorted by log_id; a repeated log_id is an error."""
    merged: dict[str, dict] = {}
    for p in paths:
        records = json.loads(Path(p).read_text(encoding="utf-8"))
        if not isinstance(records, list):
            raise ValueError(f"{Path(p).name}: expected a JSON list of per-log records")
        for rec in records:
            log_id = rec["log_id"]
            if log_id in merged:
                raise ValueError(f"log_id appears in more than one part: {log_id}")
            merged[log_id] = rec
    return [merged[k] for k in sorted(merged)]


def recap_per_vehicle(records: Sequence[dict], vehicle_of: Mapping[str, str],
                      max_per_vehicle: int) -> list[dict]:
    """The records kept by the per-vehicle cap over all of them, in their order.

    The cap is `manifest.cap_per_vehicle`, the rule each pass applies to its own
    chunk; applied again to the merged records it keeps the same logs as one
    pass over every chunk. A record whose log has no manifest row is kept."""
    listed = [r["log_id"] for r in records if r["log_id"] in vehicle_of]
    kept = set(cap_per_vehicle(listed, vehicle_of, max_per_vehicle))
    return [r for r in records if r["log_id"] not in vehicle_of or r["log_id"] in kept]


def _merge_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m log_validation.outputs")
    sub = parser.add_subparsers(dest="command", required=True)
    merge = sub.add_parser("merge", help="merge per-log JSON parts into one file")
    merge.add_argument("-o", "--output", type=Path, required=True)
    merge.add_argument("--manifest", type=Path, default=None, metavar="PATH",
                       help="CSV with columns log_id,vehicle_id; apply the per-vehicle cap "
                            "again over all parts (logs without a row are kept)")
    merge.add_argument("--max-per-vehicle", type=int, default=None, metavar="K",
                       help=f"keep the K smallest log IDs of each vehicle (default "
                            f"{DEFAULT_MAX_PER_VEHICLE}; only with --manifest)")
    merge.add_argument("parts", nargs="+", type=Path, metavar="PART")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _merge_parser()
    args = parser.parse_args(argv)
    if args.max_per_vehicle is not None and args.manifest is None:
        parser.error("--max-per-vehicle needs --manifest")
    try:
        records = merge_per_log(args.parts)
        if args.manifest is not None:
            cap = DEFAULT_MAX_PER_VEHICLE if args.max_per_vehicle is None else args.max_per_vehicle
            kept = recap_per_vehicle(records, read_manifest(args.manifest), cap)
            print(f"Per-vehicle cap of {cap}: dropped {len(records) - len(kept)} of "
                  f"{len(records)} entries", file=sys.stderr)
            records = kept
        write_json(args.output, records, forbidden_strings())
    except (ValueError, OSError, KeyError) as e:
        print(f"Error: {e}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
