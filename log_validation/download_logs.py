"""Download public PX4 flight-review logs for sensor-model validation.

Standalone trim of the upstream flight_review downloader. The upstream script
imports `plot_app.config_tables` (flight_review-internal) solely for the
--flight-modes / --error-labels filters; those are dropped here. The filters
that matter for noise validation (mav type, airframe type, rating,
latest-per-vehicle) are kept, along with the upstream rate-limit/backoff logic.

Be a good citizen: the review.px4.io service is donor-funded and rate-limited.
Keep --max-num small and leave the default delay in place.
"""

from __future__ import annotations

import argparse
import datetime
import glob
import os
import sys
import time

import requests

DEFAULT_DELAY_SECONDS = 6  # ~10 requests/minute
DEFAULT_MAX_NUM = 10
DB_INFO_API = "https://review.px4.io/dbinfo"
DOWNLOAD_API = "https://review.px4.io/download"


def get_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    p.add_argument("--max-num", "-n", type=int, default=DEFAULT_MAX_NUM,
                   help="Max files to download (-1 = all matched).")
    p.add_argument("-d", "--download-folder", default="data/downloaded/",
                   help="Folder to store downloaded .ulg files.")
    p.add_argument("--overwrite", action="store_true", default=False)
    p.add_argument("--mav-type", nargs="+", default=None,
                   help="Filter by mav type, e.g. Quadrotor (case-insensitive).")
    p.add_argument("--rating", nargs="+", default=None,
                   help="Filter by rating, e.g. good.")
    p.add_argument("--airframe-type", default=None,
                   help="Filter by airframe type, e.g. 'Quadrotor X'.")
    p.add_argument("--latest-per-vehicle", action="store_true",
                   help="Keep only the newest log per vehicle uuid.")
    p.add_argument("--delay", type=float, default=DEFAULT_DELAY_SECONDS,
                   help="Seconds between downloads (respect server limits).")
    return p.parse_args()


def download_with_retry(entry_id: str, max_retries: int = 5):
    url = DOWNLOAD_API + "?log=" + entry_id
    for attempt in range(max_retries):
        try:
            r = requests.get(url, stream=True, timeout=600)
            if r.status_code == 503:
                wait = int(r.headers.get("Retry-After", min(30 * 2**attempt, 300)))
                print(f"  rate limited (503), waiting {wait}s")
                time.sleep(wait)
                continue
            if r.status_code in (403, 444):
                print(f"  IP blocked (HTTP {r.status_code}). Stop and back off.")
                sys.exit(1)
            if r.status_code == 404:
                print("  not found (404), skipping")
                return None
            if r.status_code != 200:
                print(f"  unexpected status {r.status_code}, retrying")
                time.sleep(10)
                continue
            return r
        except requests.exceptions.RequestException as exc:
            print(f"  request failed: {exc} (retry {attempt + 1}/{max_retries})")
            time.sleep(10 * (attempt + 1))
    print("  failed after retries, skipping")
    return None


def filter_entries(entries: list[dict], args: argparse.Namespace) -> list[dict]:
    if args.mav_type:
        wanted = {m.lower() for m in args.mav_type}
        entries = [e for e in entries if e.get("mav_type", "").lower() in wanted]
    if args.rating:
        wanted = {r.lower() for r in args.rating}
        entries = [e for e in entries if e.get("rating", "").lower() in wanted]
    if args.airframe_type:
        entries = [e for e in entries if e.get("airframe_type") == args.airframe_type]
    if args.latest_per_vehicle:
        latest: dict[str, dict] = {}
        for e in entries:
            uuid = e.get("vehicle_uuid")
            if not uuid:
                continue
            d = _log_date(e)
            if uuid not in latest or d > _log_date(latest[uuid]):
                latest[uuid] = e
        entries = list(latest.values())
    # Decorate-sort-undecorate: each date is parsed once rather than on every
    # comparison the sort makes. `strptime` is about 10 µs, and an O(n log n)
    # sort over a 10k-entry catalogue called it roughly 130,000 times.
    dated = [(_log_date(e), e) for e in entries]
    dated.sort(key=lambda pair: pair[0], reverse=True)
    return [e for _, e in dated]


def _log_date(entry: dict) -> datetime.datetime:
    """Parse an entry's log_date. Cached, because the same entries are compared
    repeatedly while deduplicating and sorting."""
    return datetime.datetime.strptime(entry["log_date"], "%Y-%m-%d")


def main() -> int:
    args = get_args()
    print("Fetching database info...")
    entries = requests.get(DB_INFO_API, timeout=300).json()
    print(f"{len(entries)} total public logs.")

    entries = filter_entries(entries, args)
    print(f"{len(entries)} match filters.")

    os.makedirs(args.download_folder, exist_ok=True)
    existing = {
        os.path.splitext(os.path.basename(f))[0]
        for f in glob.glob(os.path.join(args.download_folder, "*.ulg"))
    }

    n = len(entries) if args.max_num < 0 else min(len(entries), args.max_num)
    downloaded = skipped = failed = 0
    for i in range(n):
        log_id = entries[i]["log_id"]
        if not args.overwrite and log_id in existing:
            skipped += 1
            continue
        print(f"Downloading {i + 1}/{n} ({log_id})")
        r = download_with_retry(log_id)
        if r is None:
            failed += 1
            continue
        out = os.path.join(args.download_folder, log_id + ".ulg")
        with open(out, "wb") as fh:
            # 64 KB chunks, not 1 KB: at the corpus average of 8.5 MB a log
            # that is about 8,700 read/write round trips per file.
            for chunk in r.iter_content(chunk_size=1 << 16):
                if chunk:
                    fh.write(chunk)
        downloaded += 1
        if i < n - 1:
            time.sleep(args.delay)

    print(f"\nDone: {downloaded} downloaded, {skipped} skipped, {failed} failed -> "
          f"{args.download_folder}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
