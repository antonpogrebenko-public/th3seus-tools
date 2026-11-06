"""Counts of logs analyzed, skipped and failed per pass, with normalized reasons.

An entry is *analyzed* when its log ID is in the pass's contributing list (see
`calibration_report.log_id_lists`). Any other entry is *failed* when one of its
error texts starts with a step label followed by `failed: ` or `unavailable: `, or
with `worker process died` (see `FAILURE_PATTERN`), and *skipped* otherwise; a
word elsewhere in a note, such as a topic's field list, does not count.
The battery result keeps only totals, so its counts come from those totals; they
partition the logs the battery pass read (see `battery_counts`).
"""

from __future__ import annotations

import re
from collections import Counter
from dataclasses import dataclass
from typing import Any, Mapping, Sequence

FAILURE_PATTERN = re.compile(r"worker process died|[A-Za-z][\w .-]* (?:failed|unavailable): ")
_PARENTHESIZED = re.compile(r"\([^()]*\)")
# The list of names ulog_loader appends to a missing topic or field, such as
# "Available topics: ['sensor_accel', 'sensor_gyro']"; it differs from log to log.
_AVAILABLE_LIST = re.compile(r"\s*Available \w+: \[(?:'[^']*'(?:, )?)*\]")
_NUMBER = re.compile(r"[-+]?\d+(?:\.\d+)?(?:[eE][-+]?\d+)?")
NUMBER_MARK = "N"


@dataclass(frozen=True)
class PassCounts:
    """Logs of one pass; `failed` is None when the pass cannot tell failed from skipped."""

    name: str
    analyzed: int
    skipped: int
    failed: int | None
    reasons: tuple[tuple[str, int], ...]  # (normalized reason, logs), most common first


def normalize_reason(text: str) -> str:
    """Key for counting error reasons: details and name lists dropped, numbers as "N".

    Response errors carry measured values, for example "roll: low excitation
    (std 0.001 < 0.02)", and noise errors the log's topics or fields
    ("Available topics: [...]"); counting exact strings would give one reason
    per log."""
    stripped = _AVAILABLE_LIST.sub("", text)
    while True:
        shorter = _PARENTHESIZED.sub("", stripped)
        if shorter == stripped:
            break
        stripped = shorter
    return " ".join(_NUMBER.sub(NUMBER_MARK, stripped).split())


def _ranked(counter: Counter[str]) -> tuple[tuple[str, int], ...]:
    return tuple(sorted(counter.items(), key=lambda item: (-item[1], item[0])))


def entry_counts(name: str, entries: Sequence[Mapping[str, Any]],
                 contributing: Sequence[str]) -> PassCounts:
    """Analyzed, skipped and failed logs of a per-log pass, and each reason once per log."""
    contributed = set(contributing)
    analyzed = skipped = failed = 0
    reasons: Counter[str] = Counter()
    for entry in entries:
        errors = [str(e) for e in entry.get("errors") or []]
        reasons.update({normalize_reason(e) for e in errors})
        if entry.get("log_id") in contributed:
            analyzed += 1
        elif any(FAILURE_PATTERN.match(e) for e in errors):
            failed += 1
        else:
            skipped += 1
    return PassCounts(name, analyzed, skipped, failed, _ranked(reasons))


# Battery reason rows and the battery JSON total each one counts.
BATTERY_REASONS = (("simulation log excluded", "sim_log_count"),
                   ("load error or no usable battery data", "skipped_log_count"),
                   ("cell count does not match the pack voltage", "cell_count_mismatch_count"),
                   ("fit log without a usable step or OCV sample", "fit_logs_without_samples"),
                   ("holdout log not scored", "holdout_logs_not_scored"),
                   ("log without a manifest row, left out", "logs_without_manifest_row"))


def battery_counts(battery: Mapping[str, Any], contributing: Sequence[str]) -> PassCounts:
    """Battery pass counts from the fit's totals, one row per log the pass read.

    The logs read are the selected logs and those without a manifest row, which
    the pass counts and leaves out. Analyzed are `contributing` (the fit logs with
    a sample and the holdout logs scored by either check); every other log is in
    exactly one reason row, so analyzed + skipped is the number read. Load errors
    cannot be told from unusable logs, so failed is None (counted with skipped)."""
    reasons = Counter({reason: int(battery.get(key) or 0) for reason, key in BATTERY_REASONS})
    return PassCounts("battery", len(contributing), sum(reasons.values()), None,
                      _ranked(Counter({k: v for k, v in reasons.items() if v > 0})))
