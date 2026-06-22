"""CLI: validate hitl-sensors noise models against real PX4 .ulg logs.

Usage:
    python -m log_validation <path>            # file or folder of .ulg
    python -m log_validation <path> --list-topics
"""

from __future__ import annotations

import argparse
import glob
import os
import sys
from collections import defaultdict

import numpy as np

from . import report
from . import ulog_loader as ul


def _ulg_paths(path: str) -> list[str]:
    if os.path.isdir(path):
        return sorted(glob.glob(os.path.join(path, "*.ulg")))
    return [path]


def _fmt(x: float | None) -> str:
    if x is None:
        return "—"
    if x == 0:
        return "0"
    return f"{x:.4g}"


def _print_rows(rows: list[report.Row], title: str) -> None:
    print(f"\n== {title} ==")
    print(
        f"{'sensor':6} {'param':24} {'model':>10} {'measured':>10} "
        f"{'ratio':>7} {'verdict':>7}  note"
    )
    for r in rows:
        ratio = "—" if r.ratio is None else f"{r.ratio:.2f}x"
        print(
            f"{r.sensor:6} {r.param:24} {_fmt(r.model):>10} {_fmt(r.measured):>10} "
            f"{ratio:>7} {r.verdict:>7}  {r.note}"
        )


def _list_topics(path: str) -> None:
    ulog = ul.load(path)
    print(f"\nTopics in {os.path.basename(path)}:")
    for name, count in sorted(ul.topic_names(ulog).items()):
        print(f"  {name:40} {count:>8} samples")


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("path", help="A .ulg file or a folder containing .ulg files")
    ap.add_argument("--list-topics", action="store_true", help="List topics and exit")
    args = ap.parse_args(argv)

    paths = _ulg_paths(args.path)
    if not paths:
        print(f"No .ulg files found at {args.path}", file=sys.stderr)
        return 2

    if args.list_topics:
        _list_topics(paths[0])
        return 0

    # measured value per (sensor, param) aggregated across logs
    agg: dict[tuple[str, str], list[float]] = defaultdict(list)
    model_of: dict[tuple[str, str], tuple[float, str, str]] = {}
    n_ok = 0

    for p in paths:
        try:
            ulog = ul.load(p)
            rows = report.analyze(ulog)
        except Exception as exc:  # noqa: BLE001 - report and continue per file
            print(f"\n!! {os.path.basename(p)}: failed to analyze: {exc}", file=sys.stderr)
            continue
        _print_rows(rows, os.path.basename(p))
        n_ok += 1
        for r in rows:
            key = (r.sensor, r.param)
            model_of[key] = (r.model, r.unit, r.note)
            if r.measured is not None:
                agg[key].append(r.measured)

    if n_ok == 0:
        print("No logs analyzed successfully.", file=sys.stderr)
        return 1

    if len(paths) > 1:
        summary = []
        for key, vals in agg.items():
            model, unit, note = model_of[key]
            med = float(np.median(vals))
            summary.append(
                report.Row(
                    sensor=key[0],
                    param=key[1],
                    model=model,
                    measured=med,
                    unit=unit,
                    verdict=report._verdict(med, model),
                    note=f"median of {len(vals)} logs",
                )
            )
        _print_rows(summary, f"AGGREGATE (median over {n_ok} logs)")

    print(
        "\nVerdict band: 'ok' = measured within 1/3x..3x of model. "
        "'FLAG' = model likely mis-set. Bias rows are approximate (Allan floor)."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
