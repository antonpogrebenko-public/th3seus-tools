"""Guarded, deterministic output writing: no private strings reach a written file."""

from __future__ import annotations

import argparse
import getpass
import gzip
import io
import json
import math
import os
import shutil
import socket
import sys
import tempfile
import zlib
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

from .manifest import DEFAULT_MAX_PER_VEHICLE, cap_per_vehicle, read_manifest

MIN_FORBIDDEN_LEN = 3
GZIP_OVER_BYTES = 10 * 1024 * 1024
GZIP_LEVEL = 9
GZIP_SUFFIX = ".gz"


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


def _dumps(cleaned: Any, ensure_ascii: bool) -> str:
    return json.dumps(cleaned, indent=1, sort_keys=True, allow_nan=False,
                      ensure_ascii=ensure_ascii) + "\n"


def json_text(obj: Any) -> str:
    """The text `write_json` writes: sorted keys, indent 1, NaN/inf as null, one newline,
    ASCII only (any other character is written as a \\u escape)."""
    return _dumps(_clean(obj), ensure_ascii=True)


def _checked_json_text(obj: Any, forbidden: Sequence[str]) -> str:
    """`json_text(obj)` after the guard has checked the unescaped text: a forbidden string
    with non-ASCII characters (a home folder such as /home/Jos\\u00e9) appears escaped in
    the written text, so the guard looks at the text before escaping."""
    cleaned = _clean(obj)
    _check(_dumps(cleaned, ensure_ascii=False), forbidden)
    return _dumps(cleaned, ensure_ascii=True)


def write_json(path: Path, obj: Any, forbidden: Sequence[str]) -> None:
    write_text(path, _checked_json_text(obj, forbidden), forbidden)


def _deterministic_gzip(data: bytes) -> bytes:
    """Gzip with a zero modification time, no file name and the fixed OS byte (255), so
    the header is fixed and the same text gives the same bytes with the same zlib build.
    Another zlib build may compress the same text differently; the decompressed text is
    the same."""
    buffer = io.BytesIO()
    with gzip.GzipFile(fileobj=buffer, mode="wb", compresslevel=GZIP_LEVEL, mtime=0) as gz:
        gz.write(data)
    return buffer.getvalue()


def write_json_or_gz(path: Path, obj: Any, forbidden: Sequence[str],
                     over_bytes: int = GZIP_OVER_BYTES) -> Path:
    """Write `obj` as JSON to `path`, or to `path` + ".gz" (deterministic gzip) when the
    text (ASCII, as `json_text`) is larger than `over_bytes`. The private-string guard
    checks the unescaped text and the written text before either is compressed. Returns
    the path written."""
    text = _checked_json_text(obj, forbidden)
    _check(text, forbidden)
    data = text.encode("ascii")
    path = Path(path)
    if len(data) <= over_bytes:
        write_text(path, text, ())
        return path
    packed = path.with_name(path.name + GZIP_SUFFIX)
    packed.parent.mkdir(parents=True, exist_ok=True)
    packed.write_bytes(_deterministic_gzip(data))
    return packed


def read_json(path: Path) -> Any:
    """Parse a JSON file, decompressing it first when the name ends in ".gz".

    ValueError for a corrupt or truncated gzip file, invalid UTF-8 or invalid JSON."""
    path = Path(path)
    raw = path.read_bytes()
    try:
        if path.name.endswith(GZIP_SUFFIX):
            raw = gzip.decompress(raw)
        return json.loads(raw.decode("utf-8"))
    except (gzip.BadGzipFile, zlib.error, EOFError) as e:
        raise ValueError(f"{path.name} is not a readable gzip file ({type(e).__name__})") from e
    except UnicodeDecodeError as e:
        raise ValueError(f"{path.name} is not valid UTF-8 text") from e
    except json.JSONDecodeError as e:
        raise ValueError(f"{path.name} is not valid JSON: {e.msg} (line {e.lineno})") from e


def input_forbidden(inputs: Sequence[Path], extra: Sequence[str] = ()) -> tuple[str, ...]:
    """Home, user, host and the input folders, plus each extra as a literal string.

    Each input's folder is forbidden both as given (made absolute, symlinks kept)
    and resolved, so a folder reached through a symlink is caught either way.
    An extra is a cloud account ID or a bucket name, not a path, so it is not
    resolved the way `forbidden_strings` resolves its arguments."""
    if any(len(s) < MIN_FORBIDDEN_LEN for s in extra):
        raise ValueError(f"a --forbid string needs at least {MIN_FORBIDDEN_LEN} characters")
    folders = sorted({folder for p in inputs
                      for folder in (Path(p).absolute().parent, Path(p).resolve().parent)})
    return (*forbidden_strings(*folders), *extra)


def check_out_dir(out_dir: Path) -> None:
    """ValueError unless `out_dir` is absent or an empty folder."""
    out = Path(out_dir)
    if out.exists() and (not out.is_dir() or any(out.iterdir())):
        raise ValueError("the output directory exists and is not empty; use a new or empty one")


def write_staged(out_dir: Path, stage_files: Callable[[Path], None]) -> None:
    """Run `stage_files(stage)` on a temporary folder, then move it into `out_dir`.

    `stage_files` writes every file with `write_text` / `write_json`, so a refused
    write (PrivateDataError) or any other error leaves `out_dir` as it was.
    `out_dir` must be new or empty (ValueError otherwise). A new `out_dir` is made by
    renaming a complete copy; an existing one is filled in place (see `_copy_into`)."""
    out = Path(out_dir)
    check_out_dir(out)
    with tempfile.TemporaryDirectory() as tmp:
        stage = Path(tmp) / "staged"
        stage.mkdir()
        stage_files(stage)
        if out.exists():
            _copy_into(stage, out)
        else:
            _rename_into_place(stage, out)


def _rename_into_place(stage: Path, out: Path) -> None:
    """Copy `stage` into a hidden temporary folder beside `out`, then rename it to `out`.

    The rename stays on one file system, so a copy that fails half way (a full disk)
    leaves no `out` and removes the partial copy. A run killed before the rename leaves
    the hidden folder (`.<name>.<random>`) behind."""
    out.parent.mkdir(parents=True, exist_ok=True)
    beside = Path(tempfile.mkdtemp(prefix=f".{out.name}.", dir=out.parent))
    try:
        copy = beside / "copy"
        shutil.copytree(stage, copy)
        copy.rename(out)
    finally:
        shutil.rmtree(beside, ignore_errors=True)


def _copy_into(stage: Path, out: Path) -> None:
    """Copy the staged entries into the existing empty folder `out`, which keeps its mode
    and owner (and stays a symlink when it is one; its parent need not be writable).

    On any error every entry the copy made is removed, so `out` is left empty as it was,
    and the error is raised again."""
    made: list[Path] = []
    try:
        for entry in sorted(stage.iterdir()):
            target = out / entry.name
            made.append(target)
            if entry.is_dir():
                shutil.copytree(entry, target)
            else:
                shutil.copy2(entry, target)
    except BaseException:
        for target in made:
            _remove_made(target)
        raise


def _remove_made(path: Path) -> None:
    """Remove a file or folder the copy made; a failure here must not hide the copy's error,
    which is raised next."""
    if path.is_dir() and not path.is_symlink():
        shutil.rmtree(path, ignore_errors=True)
    elif path.exists() or path.is_symlink():
        try:
            path.unlink()
        except OSError:
            pass


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
