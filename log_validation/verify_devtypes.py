"""Audit the static IMU devtype table against each log's actual PX4 firmware.

PX4 can reassign DRV_*_DEVTYPE_* hex values across releases, so a devtype
decoded with the wrong firmware's table could silently mislabel a chip. Every
log records its firmware commit (`ver_sw`), so we fetch that exact commit's
`src/drivers/drv_sensor.h`, build the per-firmware devtype table, and compare
the resulting chip name to what `hardware.py`'s static table produced.

This is a maintenance/audit tool: it uses the network and is never imported by
the offline analysis path. Run it after refreshing the dataset or the table.

Usage:
    python -m log_validation.verify_devtypes data/results_full_v2.json
"""

from __future__ import annotations

import argparse
import json
import re
import urllib.error
import urllib.request
from difflib import SequenceMatcher
from pathlib import Path

# Names this similar are treated as a spelling variant of the same part (e.g.
# the upstream "FXAS2100C" typo vs the corrected "FXAS21002C"), not a genuine
# devtype reassignment to a different chip (e.g. ICM20602 vs ICM20689 ~ 0.75).
SPELLING_VARIANT_RATIO = 0.85


def _same_part(a: str, b: str) -> bool:
    return SequenceMatcher(None, a.upper(), b.upper()).ratio() >= SPELLING_VARIANT_RATIO

RAW_URL = "https://raw.githubusercontent.com/PX4/PX4-Autopilot/{ref}/src/drivers/drv_sensor.h"
DEVTYPE_RE = re.compile(r"#define\s+DRV_(\w+?)_DEVTYPE_(\w+)\s+(0x[0-9A-Fa-f]+)")


def fetch_devtype_table(ref: str) -> dict[int, str] | None:
    """Fetch and parse the IMU/gyro/accel devtype table at a git ref.

    Returns None if the ref is not on upstream PX4 (e.g. a vendor fork build).
    """
    try:
        txt = urllib.request.urlopen(RAW_URL.format(ref=ref), timeout=25).read().decode(
            errors="replace"
        )
    except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError):
        return None

    table: dict[int, str] = {}
    for group, name, hexval in DEVTYPE_RE.findall(txt):
        if group in ("IMU", "GYR", "ACC"):
            table.setdefault(int(hexval, 16), name)  # first definition wins
    return table or None


def primary_devtype(hardware: dict) -> int | None:
    gyro_ids = hardware.get("gyro_ids") or []
    accel_ids = hardware.get("accel_ids") or []
    ids = gyro_ids or accel_ids
    if not ids:
        return None
    return (int(ids[0]) >> 16) & 0xFF


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("results", type=Path, help="batch_analyze results JSON")
    ap.add_argument(
        "--all",
        action="store_true",
        help="check every log (default: only measurement-bearing logs)",
    )
    args = ap.parse_args(argv)

    records = json.loads(args.results.read_text())
    logs = records if args.all else [r for r in records if r.get("measurements")]

    cache: dict[str, dict[int, str] | None] = {}

    def table_for(ref: str | None) -> dict[int, str] | None:
        if not ref:
            return None
        if ref not in cache:
            cache[ref] = fetch_devtype_table(ref)
        return cache[ref]

    main_table = fetch_devtype_table("main") or {}
    _ = main_table  # fetched to warm the comparison reference / sanity-check network

    discrepancies: list[tuple] = []  # genuine reassignment to a different chip
    variants: list[tuple] = []  # same part, different spelling across firmware
    verified = unverified = 0

    for r in logs:
        hw = r.get("hardware") or {}
        ref = hw.get("px4_version")
        devtype = primary_devtype(hw)
        if devtype is None:
            continue
        fw_table = table_for(ref)
        if fw_table is None:
            unverified += 1
            continue
        verified += 1
        fw_name = fw_table.get(devtype)
        static_name = hw.get("imu_name")
        # Static decode may carry a SIM/UNKNOWN label; only flag real mismatches.
        if (
            fw_name
            and static_name
            and fw_name != static_name
            and not static_name.startswith("UNKNOWN")
        ):
            row = (ref[:8], f"0x{devtype:02X}", fw_name, static_name, hw.get("fc_board"))
            (variants if _same_part(fw_name, static_name) else discrepancies).append(row)

    print(f"logs checked:          {len(logs)}")
    print(f"verified vs own fw:    {verified}")
    print(f"unverified (fork/404): {unverified}")
    print(f"spelling variants:     {len(variants)}")
    for ref, hexv, fw_name, static_name, board in variants:
        print(f"  ~ {ref} {hexv} firmware={fw_name} static={static_name} board={board}")
    print(f"discrepancies:         {len(discrepancies)}")
    for ref, hexv, fw_name, static_name, board in discrepancies:
        print(f"  ! {ref} {hexv} firmware={fw_name} static={static_name} board={board}")

    # Non-zero exit only on a genuine reassignment so this can gate CI.
    return 1 if discrepancies else 0


if __name__ == "__main__":
    raise SystemExit(main())
