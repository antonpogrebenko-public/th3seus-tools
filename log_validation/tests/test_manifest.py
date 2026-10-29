"""Manifest reading and per-vehicle log selection."""

from __future__ import annotations

import argparse
from pathlib import Path

import pytest

from log_validation.manifest import (
    ManifestError,
    add_selection_args,
    read_manifest,
    select_logs,
)


def _make_logs(root: Path, names: list[str]) -> Path:
    root.mkdir(parents=True, exist_ok=True)
    for name in names:
        (root / f"{name}.ulg").write_bytes(b"")
    return root


def _make_manifest(path: Path, rows: list[tuple[str, str]]) -> Path:
    lines = ["log_id,vehicle_id"] + [f"{a},{b}" for a, b in rows]
    path.write_text("\n".join(lines) + "\n")
    return path


def test_manifest_with_missing_log(tmp_path):
    letters = list("abcdefghij")
    logs = _make_logs(tmp_path / "logs", letters[:9])
    manifest = _make_manifest(
        tmp_path / "m.csv", [(c, f"v{i}") for i, c in enumerate(letters)]
    )
    sel = select_logs(logs, manifest)
    assert len(sel.paths) == 9
    assert sel.manifest_rows_without_log == 1


def test_cap_keeps_smallest_ids(tmp_path):
    names = [f"l{i}" for i in range(6, 0, -1)]
    logs = _make_logs(tmp_path / "logs", names)
    manifest = _make_manifest(tmp_path / "m.csv", [(n, "v1") for n in names])
    sel = select_logs(logs, manifest, max_per_vehicle=3)
    assert [p.stem for p in sel.paths] == ["l1", "l2", "l3"]
    assert sel.capped_out == 3
    assert sel.vehicle_of == {"l1": "v1", "l2": "v1", "l3": "v1"}


def test_log_not_in_manifest_is_left_out(tmp_path):
    logs = _make_logs(tmp_path / "logs", ["a", "extra"])
    manifest = _make_manifest(tmp_path / "m.csv", [("a", "v1")])
    sel = select_logs(logs, manifest)
    assert sel.logs_without_manifest_row == 1
    assert "extra" not in {p.stem for p in sel.paths}


def test_without_manifest_every_log_counts(tmp_path):
    names = ["c", "a", "b", "d", "e"]
    logs = _make_logs(tmp_path / "logs", names)
    sel = select_logs(logs)
    assert [p.stem for p in sel.paths] == sorted(names)
    assert sel.vehicle_of == {}
    assert sel.capped_out == 0


def test_cap_is_per_vehicle(tmp_path):
    logs = _make_logs(tmp_path / "logs", ["a1", "a2", "b1"])
    manifest = _make_manifest(
        tmp_path / "m.csv", [("a1", "v1"), ("a2", "v1"), ("b1", "v2")]
    )
    sel = select_logs(logs, manifest, max_per_vehicle=1)
    assert [p.stem for p in sel.paths] == ["a1", "b1"]
    assert sel.capped_out == 1


def test_manifest_format(tmp_path):
    path = tmp_path / "m.csv"
    path.write_text("log_id,vehicle_id\n a , v1 \n\nb,v2\n")
    assert read_manifest(path) == {"a": "v1", "b": "v2"}


def test_manifest_repeated_log_id(tmp_path):
    path = tmp_path / "m.csv"
    path.write_text("log_id,vehicle_id\nabc,v1\nabc,v2\n")
    with pytest.raises(ManifestError, match="abc"):
        read_manifest(path)


def test_manifest_missing_column(tmp_path):
    path = tmp_path / "m.csv"
    path.write_text("log_id,other\na,v1\n")
    with pytest.raises(ManifestError, match="vehicle_id"):
        read_manifest(path)


def test_selection_args_defaults(tmp_path):
    parser = argparse.ArgumentParser()
    add_selection_args(parser)
    args = parser.parse_args([])
    assert args.manifest is None
    assert args.max_per_vehicle == 3
    assert args.jobs == 1
    args = parser.parse_args(["--manifest", "m.csv", "--max-per-vehicle", "2", "--jobs", "4"])
    assert (args.manifest, args.max_per_vehicle, args.jobs) == (Path("m.csv"), 2, 4)


def test_cap_per_vehicle_keeps_the_smallest_ids_of_each_vehicle():
    from log_validation.manifest import cap_per_vehicle

    vehicle_of = {"b2": "v1", "a1": "v1", "a2": "v1", "c1": "v2", "a3": "v1"}
    assert cap_per_vehicle(vehicle_of, vehicle_of, 2) == ["a1", "a2", "c1"]
    assert cap_per_vehicle(vehicle_of, vehicle_of, 0) == []


def test_negative_cap_is_refused(tmp_path):
    log_dir = _make_logs(tmp_path / "logs", ["1", "2"])
    with pytest.raises(ValueError, match="max_per_vehicle"):
        select_logs(log_dir, None, -1)
    parser = argparse.ArgumentParser()
    add_selection_args(parser)
    with pytest.raises(SystemExit):
        parser.parse_args(["--max-per-vehicle", "-1"])
