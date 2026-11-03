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
from types import MappingProxyType

from .part_notes import existing_arm_comment
from .part_rows import STATUS_CANDIDATE, PartRow, PartTables
from .th3seus_parts import (
    FIX_CLASS_RTK_FIXED, FIX_CLASS_STANDALONE_3D, HITL_DAEMON_VERSION, HITL_PHYSICS_VERSION,
    OTHER, PARTS, PartType, Variant, current_literal, daemon_parse, normalize, variant)

__all__ = ["ALIASES_FILE", "FIX_CLASS_NOTES", "RUST_FILES", "emit_daemon_aliases", "emit_part_source",
           "emit_rust_files", "is_plain", "rust_f64"]

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
MAX_DOC_LINE = 100
FIX_CLASS_NOTES: Mapping[str, str] = MappingProxyType({
    FIX_CLASS_STANDALONE_3D: "measured over standalone 3D fixes",
    FIX_CLASS_RTK_FIXED: "measured over RTK fixed solutions"})


def is_plain(text: str) -> bool:
    """Printable ASCII without `"` and `\\`: safe in a Rust string literal and a line comment."""
    return PLAIN_ALIAS.fullmatch(text) is not None


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
    """Candidate variants with a proposal, sorted by name; names checked before any emit.

    Read from the candidate rows themselves, not grouped by name: a candidate that shares
    its name with Other or an existing variant (a log named "OTHER") would hide there."""
    spec = PARTS[part]
    existing = {v.name for v in spec.variants} | set(spec.no_profile_variants)
    names = sorted({r.variant for r in tables.rows
                    if r.part == part and r.status == STATUS_CANDIDATE and r.proposed is not None})
    for name in names:
        if not IDENTIFIER.fullmatch(name) or name in RESERVED_VARIANTS:
            raise ValueError(f"{part}: variant name {name!r} is not a usable Rust identifier")
        if name in existing:
            raise ValueError(f"{part}: new variant {name!r} collides with an existing variant")
    return tuple(names)


def _log_names(rows: Mapping[str, PartRow]) -> tuple[str, ...]:
    return tuple(sorted({n for r in rows.values() for n in r.log_names}))


def _comment_names(part: str, rows: Mapping[str, PartRow]) -> tuple[str, ...]:
    """The log names of a new variant, for its comment; ValueError for a name that is not
    plain printable ASCII (a line break would end the comment, and every output is ASCII)."""
    names = _log_names(rows)
    for name in names:
        _check_plain(part, "log name", name)
    return names


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


def _fix_class_note(part: str, name: str, rows: Mapping[str, PartRow]) -> str:
    """The GPS fix-class note of an arm with a proposal; "" for another part or no proposal.

    ValueError when the proposed rows come from two classes or from one that has no note."""
    if part != GPS:
        return ""
    classes = {r.fix_class for r in rows.values() if r.proposed is not None}
    if not classes:
        return ""
    if len(classes) > 1:
        raise ValueError(f"gps: {name} has proposed values from fix classes "
                         f"{sorted(map(str, classes))}")
    (cls,) = classes
    if cls not in FIX_CLASS_NOTES:
        raise ValueError(f"gps: {name} has proposed values from an unknown fix class {cls!r}")
    return FIX_CLASS_NOTES[cls]


def _existing_comment(spec: PartType, name: str, rows: Mapping[str, PartRow]) -> str:
    measured = frozenset(f for f, r in rows.items() if r.proposed is not None)
    pieces = (existing_arm_comment(spec.key, name, measured),
              _fix_class_note(spec.key, name, rows))
    return "; ".join(p for p in pieces if p)


def _new_comment(spec: PartType, name: str, rows: Mapping[str, PartRow], run_date: str) -> str:
    names = ", ".join(_comment_names(spec.key, rows))
    pieces = (f"PX4 log names {names}; new in run {run_date}", _fix_class_note(spec.key, name, rows))
    return "; ".join(p for p in pieces if p)


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
        arm_rows = rows.get(base.name, {})
        lines += _arm(spec, base.name, base, _existing_comment(spec, base.name, arm_rows),
                      arm_rows, old_existing, run_date)
    other = next(v for v in spec.variants if v.name == OTHER)
    for name in _new_variants(tables, spec.key):
        lines += _arm(spec, name, other, _new_comment(spec, name, rows[name], run_date),
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


def _doc_lines(spec: PartType, run_date: str) -> list[str]:
    """The profile function doc: the mirrored summary and sources, with the provenance rebuilt."""
    summary = spec.doc[0].removeprefix("Datasheet-derived ")
    lines = [summary[:1].upper() + summary[1:], "",
             'A value with a "was" comment is the median over vehicles measured from real PX4 logs in',
             f"run {run_date}; every other value is the hitl-physics {HITL_PHYSICS_VERSION} table value."]
    sources = f"hitl-physics {HITL_PHYSICS_VERSION} sources:"
    rest = list(spec.doc[2:] if len(spec.doc) > 1 and spec.doc[1] == "" else spec.doc[1:])
    if rest:
        lines += ["", *(sources + r.removeprefix("Sources:") if r.startswith("Sources:") else r
                        for r in rest)]
    out = [f"{I1}/// {line}".rstrip() for line in lines]
    if any("Datasheet-derived" in line for line in out):
        raise ValueError(f"{spec.key}: doc line still says 'Datasheet-derived'")
    too_long = [line for line in out if len(line) > MAX_DOC_LINE]
    if too_long:
        raise ValueError(f"{spec.key}: doc line over {MAX_DOC_LINE} characters: {too_long[0]!r}")
    return out


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
    body = (_enum_lines(spec, new) + ["", f"impl {spec.enum} {{"] + _doc_lines(spec, run_date)
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


def _check_plain(part: str, what: str, text: str) -> None:
    if not is_plain(text):
        raise ValueError(f"{part}: {what} {text!r} is not plain printable ASCII")


def _check_alias(part: str, alias: str) -> None:
    _check_plain(part, "alias", alias)


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
            _check_plain(spec.key, "catalog log name", log_name)
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
