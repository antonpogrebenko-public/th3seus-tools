"""Markdown table helpers shared by the calibration report sections."""

from __future__ import annotations

import math
import re
from typing import Any, Sequence

NA = "n/a"
_LINE_BREAKS = re.compile(r"[\r\n]+")


def _num(value: Any, spec: str = ".4g") -> str:
    ok = isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)
    return format(value, spec) if ok else NA


def _cell(text: str) -> str:
    """A table cell: a "|" in the text (a log-controlled reason may hold one) is escaped and
    a line break becomes a space, so the row stays one line."""
    return _LINE_BREAKS.sub(" ", str(text)).replace("|", "\\|")


def _table(headers: Sequence[str], rows: Sequence[Sequence[str]]) -> list[str]:
    lines = ["| " + " | ".join(headers) + " |", "|" + "---|" * len(headers)]
    lines.extend("| " + " | ".join(_cell(c) for c in row) + " |" for row in rows)
    return lines + [""]
