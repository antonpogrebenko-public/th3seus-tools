"""Guarded, deterministic output writing: no private strings reach a written file."""

from __future__ import annotations

import argparse
import getpass
import json
import math
import socket
from pathlib import Path
from typing import Any, Sequence

MIN_FORBIDDEN_LEN = 3


class PrivateDataError(ValueError):
    """An output would contain a path, host name or user name."""


def forbidden_strings(*extra: str | Path) -> tuple[str, ...]:
    """Strings no output may contain: home dir, user, host, and each extra (as given and resolved)."""
    candidates: list[str] = [str(Path.home()), getpass.getuser(), socket.gethostname()]
    for item in extra:
        candidates.append(str(item))
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


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m log_validation.outputs")
    sub = parser.add_subparsers(dest="command", required=True)
    merge = sub.add_parser("merge", help="merge per-log JSON parts into one file")
    merge.add_argument("-o", "--output", type=Path, required=True)
    merge.add_argument("parts", nargs="+", type=Path, metavar="PART")
    args = parser.parse_args(argv)
    write_json(args.output, merge_per_log(args.parts), forbidden_strings())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
