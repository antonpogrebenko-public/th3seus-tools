"""Rust text for the sibling hitl-physics and hitl-daemon changes.

Per part type one file holds the complete enum and profile function in the
structure of hitl-physics 0.13.1 `src/build.rs`, with the proposed values and
the new variants; `daemon_aliases.rs` holds the parser alias lines for the new
variants (hitl-daemon 0.27.0 `build_config.rs`). The same inputs and run date
give byte-identical text; every ordering below is sorted or follows the part
tables, never a set.
"""

from __future__ import annotations

import math
import re
from collections.abc import Mapping, Sequence

from .part_rows import STATUS_CANDIDATE, PartRow, PartTables
from .th3seus_parts import (
    HITL_DAEMON_VERSION, HITL_PHYSICS_VERSION, OTHER, PARTS, PartType, Variant, current_literal,
    daemon_parse, normalize, variant)

__all__ = ["ALIASES_FILE", "RUST_FILES", "emit_daemon_aliases", "emit_part_source",
           "emit_rust_files", "rust_f64"]

RUST_FILES = {"imu": "imu_chip.rs", "baro": "baro_chip.rs", "mag": "mag_chip.rs",
              "gps": "gps_chipset.rs"}
ALIASES_FILE = "daemon_aliases.rs"

DERIVE = "#[derive(Debug, Clone, PartialEq, Eq)]"
IDENTIFIER = re.compile(r"[A-Z][A-Za-z0-9]*")
PLAIN_ALIAS = re.compile(r"[ !#-\[\]-~]*")  # printable ASCII without `"` and `\`
RESERVED_VARIANTS = frozenset({"Self", "None", OTHER})
EXPONENT = re.compile(r"e([+-])0*(\d+)")
I1, I2, I3, I4 = "    ", "        ", "            ", "                "
GPS = "gps"


def rust_f64(value: float) -> str:
    """Four significant digits as a Rust f64 literal (`1.235e-5`, `5.0`)."""
    if not math.isfinite(value):
        raise ValueError(f"cannot write {value!r} as a Rust float literal")
    text = EXPONENT.sub(lambda m: "e" + ("-" if m.group(1) == "-" else "") + m.group(2),
                        format(value, ".4g"))
    return text if "." in text or "e" in text else text + ".0"


def _rows_by_variant(tables: PartTables, part: str) -> dict[str, dict[str, PartRow]]:
    grouped: dict[str, dict[str, PartRow]] = {}
    for row in tables.rows:
        if row.part == part:
            grouped.setdefault(row.variant, {})[row.parameter] = row
    return grouped


def _has_proposal(rows: Mapping[str, PartRow] | None) -> bool:
    return any(r.proposed is not None for r in (rows or {}).values())


def _new_variants(tables: PartTables, part: str) -> tuple[str, ...]:
    """Candidate variants with a proposal, sorted by name; names checked before any emit."""
    spec = PARTS[part]
    existing = {v.name for v in spec.variants} | set(spec.no_profile_variants)
    names = sorted(
        name for name, rows in _rows_by_variant(tables, part).items()
        if _has_proposal(rows) and any(r.status == STATUS_CANDIDATE for r in rows.values()))
    for name in names:
        if not IDENTIFIER.fullmatch(name) or name in RESERVED_VARIANTS:
            raise ValueError(f"{part}: variant name {name!r} is not a usable Rust identifier")
        if name in existing:
            raise ValueError(f"{part}: new variant {name!r} collides with an existing variant")
    return tuple(names)


def _log_names(rows: Mapping[str, PartRow]) -> tuple[str, ...]:
    return tuple(sorted({n for r in rows.values() for n in r.log_names}))


def _field_lines(part: str, name: str, base: Variant, rows: Mapping[str, PartRow],
                 old_source: str, run_date: str) -> list[str]:
    lines = []
    for field, literal in base.fields:
        row = rows.get(field)
        if row is not None and row.proposed is not None:
            lines.append(f"{I4}// was {current_literal(part, name, field)} ({old_source}); "
                         f"median of {row.vehicles} vehicles, run {run_date}")
            literal = rust_f64(row.proposed)
        lines.append(f"{I4}{field}: {literal},")
    return lines


def _arm(spec: PartType, name: str, base: Variant, comment: str, rows: Mapping[str, PartRow],
         old_source: str, run_date: str) -> list[str]:
    lines = [f"{I3}// {comment}"] if comment else []
    pattern = f"{spec.enum}::{name}"
    if name == OTHER and base.delegate and not _has_proposal(rows):
        return lines + [f"{I3}{pattern} => {base.delegate},"]
    opener, closer = ((f"Some({spec.profile} {{", "}),") if spec.optional
                      else (f"{spec.profile} {{", "},"))
    return (lines + [f"{I3}{pattern} => {opener}"]
            + _field_lines(spec.key, name, base, rows, old_source, run_date)
            + [f"{I3}{closer}"])


def _other_comment(spec: PartType, base: Variant, rows: Mapping[str, PartRow]) -> str:
    """With a proposal, Other is TH3SEUS's fallback entry: the median over every vehicle of
    the part type, whatever its part (GPS: the logs with a standalone 3D fix)."""
    if not _has_proposal(rows):
        return base.comment
    scope = "all vehicles of this part type"
    if spec.key == GPS:
        scope += ", standalone 3D fix only"
    return f"Unknown part - median over {scope} (TH3SEUS's fallback entry)"


def _arms(spec: PartType, tables: PartTables, run_date: str) -> list[str]:
    rows = _rows_by_variant(tables, spec.key)
    old_existing = f"hitl-physics {HITL_PHYSICS_VERSION}"
    lines: list[str] = []
    for base in spec.variants:
        if base.name == OTHER:
            continue
        lines += _arm(spec, base.name, base, base.comment, rows.get(base.name, {}),
                      old_existing, run_date)
    other = next(v for v in spec.variants if v.name == OTHER)
    for name in _new_variants(tables, spec.key):
        names = ", ".join(_log_names(rows[name]))
        lines += _arm(spec, name, other, f"PX4 log names {names}; new in run {run_date}",
                      rows[name], OTHER, run_date)
    if spec.no_profile_variants:
        lines += [f"{I3}{spec.enum}::None => None,"]
    other_rows = rows.get(OTHER, {})
    lines += _arm(spec, OTHER, other, _other_comment(spec, other, other_rows), other_rows,
                  old_existing, run_date)
    return lines


def _enum_lines(spec: PartType, new: Sequence[str]) -> list[str]:
    names = ([v.name for v in spec.variants if v.name != OTHER] + list(new)
             + list(spec.no_profile_variants) + [OTHER])
    return [DERIVE, f"pub enum {spec.enum} {{"] + [f"{I1}{n}," for n in names] + ["}"]


def _doc_lines(spec: PartType) -> list[str]:
    return [f"{I1}/// {line}".rstrip() for line in spec.doc]


def emit_part_source(part_type: str, tables: PartTables, run_date: str) -> str:
    """The enum and its profile impl for one part type, in the structure of build.rs 0.13.1."""
    spec = PARTS[part_type]
    new = _new_variants(tables, part_type)
    returns = f"Option<{spec.profile}>" if spec.optional else spec.profile
    header = [
        f"// Generated by log_validation.part_tables, run {run_date}: {spec.enum} and",
        f"// {spec.enum}::{spec.profile_fn} for hitl-physics src/build.rs, in the structure of",
        f"// hitl-physics {HITL_PHYSICS_VERSION}. A \"was\" comment marks a value measured from",
        "// real PX4 logs (median over vehicles).",
        "",
    ]
    body = (_enum_lines(spec, new) + ["", f"impl {spec.enum} {{"] + _doc_lines(spec)
            + [f"{I1}pub fn {spec.profile_fn}(&self) -> {returns} {{", f"{I2}match self {{"]
            + _arms(spec, tables, run_date) + [f"{I2}}}", f"{I1}}}", "}"])
    return "\n".join(header + body) + "\n"


def _catalog_aliases(catalog: Mapping[str, str], log_names: Sequence[str]) -> list[str]:
    """The catalog spellings of these log names; log names compare after `normalize`."""
    wanted = {normalize(n) for n in log_names}
    return [normalize(spelling) for spelling, log_name in catalog.items()
            if normalize(log_name) in wanted]


def _usable_catalog(catalog: Mapping[str, str], indent: str) -> tuple[dict[str, str], list[str]]:
    """The spellings that are not empty after `normalize`, and a comment for each that is
    (an empty alias would match an empty name). Such a spelling holds only `-`, `_` and
    spaces, so it is safe in a comment. Log names cannot be empty here: an emitted variant
    has a valid identifier, so its log name has a letter or digit."""
    usable = {s: n for s, n in catalog.items() if normalize(s)}
    skipped = [f'{indent}// skipped catalog spelling "{s}": empty after normalizing'
               for s in sorted(catalog) if not normalize(s)]
    return usable, skipped


def _check_alias(part: str, alias: str) -> None:
    if not PLAIN_ALIAS.fullmatch(alias):
        raise ValueError(f"{part}: alias {alias!r} is not plain printable ASCII")


def _alias_line(spec: PartType, name: str, aliases: Sequence[str]) -> str:
    alternatives = " | ".join('"' + a + '"' for a in aliases)
    if spec.key == GPS:
        return (f"{I1}if matches!(normalized.as_str(), {alternatives}) "
                f"{{ return {spec.enum}::{name}; }}")
    return f"{I2}{alternatives} => {spec.enum}::{name},"


def _block(spec: PartType, tables: PartTables, catalog: Mapping[str, str]) -> list[str]:
    indent = I1 if spec.key == GPS else I2
    new = _new_variants(tables, spec.key)
    rows = _rows_by_variant(tables, spec.key)
    where = ("paste these lines BEFORE the substring rules of parse_gps_chipset"
             if spec.key == GPS else "paste these arms above the `_ =>` arm")
    lines = [f"// {spec.parser}", f"// {where}"]
    lines += [f"{indent}// no new variant"] if not new else []
    catalog, skipped = _usable_catalog(catalog, indent)
    lines += skipped
    claimed: dict[str, str] = {}
    for name in new:
        names = _log_names(rows[name])
        candidates = sorted({normalize(n) for n in names}
                            | set(_catalog_aliases(catalog, names)))
        emit = []
        for alias in candidates:
            _check_alias(spec.key, alias)
            parsed = daemon_parse(spec.key, alias)
            if parsed != OTHER:
                lines.append(f"{indent}// skipped \"{alias}\": hitl-daemon {HITL_DAEMON_VERSION} "
                             f"parses it as {spec.enum}::{parsed}")
            elif alias in claimed:
                lines.append(f"{indent}// skipped \"{alias}\": already used by "
                             f"{spec.enum}::{claimed[alias]}")
            else:
                claimed[alias] = name
                emit.append(alias)
        if emit:
            lines.append(_alias_line(spec, name, emit))
    known = {normalize(n) for name in new for n in _log_names(rows[name])}
    for spelling, log_name in sorted(catalog.items(), key=lambda kv: normalize(kv[0])):
        if normalize(log_name) not in known:
            _check_alias(spec.key, normalize(spelling))
            lines.append(f"{indent}// not emitted: \"{normalize(spelling)}\" "
                         f"({log_name} has no new variant)")
    return lines


def emit_daemon_aliases(tables: PartTables, run_date: str,
                        catalog: Mapping[str, Mapping[str, str]]) -> str:
    """The parser alias lines of every part type (hitl-daemon build_config.rs)."""
    lines = [
        f"// Generated by log_validation.part_tables, run {run_date}: parser aliases for the new",
        f"// variants, for hitl-daemon {HITL_DAEMON_VERSION} crates/websocket/src/build_config.rs.",
        "// Names are in the daemon's normalized form (lowercase, no `-`, `_` or space).",
    ]
    for spec in PARTS.values():
        lines += [""] + _block(spec, tables, catalog.get(spec.key, {}))
    return "\n".join(lines) + "\n"


def emit_rust_files(tables: PartTables, run_date: str,
                    catalog: Mapping[str, Mapping[str, str]] | None = None) -> dict[str, str]:
    """The five Rust files: file name -> text."""
    files = {RUST_FILES[p]: emit_part_source(p, tables, run_date) for p in PARTS}
    files[ALIASES_FILE] = emit_daemon_aliases(tables, run_date, catalog or {})
    return files
