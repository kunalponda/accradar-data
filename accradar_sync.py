#!/usr/bin/env python3
"""
accradar_sync.py — Accumulation Radar nightly data sync
Saves to: C:/AccRadar/accradar-data/

Run via Windows Task Scheduler, daily at 8:05 PM.
Fetches 5 NSE files -> saves them to the repo -> writes dashboard-data.json
and bhavcopy_index.json -> git push.

Usage:
  python accradar_sync.py                  # fetch today's files
  python accradar_sync.py --date 26092026  # fetch a specific date (DDMMYYYY)
"""

import csv
import io
import os
import sys
import json
import subprocess
import logging
from datetime import datetime, date, timezone

import requests

# ── config ─────────────────────────────────────────────────────────────────────
REPO_DIR    = os.path.dirname(os.path.abspath(__file__))   # C:\AccRadar\accradar-data
LOG_FILE    = os.path.join(REPO_DIR, "accradar.log")
DATA_JSON   = os.path.join(REPO_DIR, "dashboard-data.json")
INDEX_JSON  = os.path.join(REPO_DIR, "bhavcopy_index.json")
BHAV_DIR    = os.path.join(REPO_DIR, "bhavcopy")

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-IN,en;q=0.9",
    "Referer": "https://www.nseindia.com/",
}

# ── logging ────────────────────────────────────────────────────────────────────
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger()

def log_and_print(msg, level="info"):
    print(msg)
    getattr(log, level)(msg)


# ── NSE URL helpers ─────────────────────────────────────────────────────────────
def bhav_url(dt: date) -> str:
    return (
        f"https://nsearchives.nseindia.com/products/content/sec_bhavdata_full_"
        f"{dt.strftime('%d%m%Y')}.csv"
    )

def indices_url(dt: date) -> str:
    return (
        f"https://nsearchives.nseindia.com/content/indices/"
        f"ind_close_all_{dt.strftime('%d%m%Y')}.csv"
    )

def wk52_url(dt: date) -> str:
    return (
        f"https://nsearchives.nseindia.com/content/"
        f"CM_52_wk_High_low_{dt.strftime('%d%m%Y')}.csv"
    )

BULK_URL  = "https://nsearchives.nseindia.com/content/equities/bulk.csv"
BLOCK_URL = "https://nsearchives.nseindia.com/content/equities/block.csv"


# ── fetch helper ───────────────────────────────────────────────────────────────
def fetch(url: str, label: str) -> bytes | None:
    """GET url with NSE headers. Returns bytes or None on 404/503/error."""
    try:
        r = requests.get(url, headers=HEADERS, timeout=30)
        if r.status_code == 404:
            log_and_print(f"  {label}: not yet published (404) — will retry at next run")
            return None
        if r.status_code == 503:
            log_and_print(f"  {label}: NSE returned 503 — will retry at next run")
            return None
        r.raise_for_status()
        log_and_print(f"  {label}: {len(r.content):,} bytes  OK")
        return r.content
    except requests.RequestException as exc:
        log_and_print(f"  {label}: fetch error — {exc}", "warning")
        return None


# ── git helper ─────────────────────────────────────────────────────────────────
def git(*args):
    result = subprocess.run(
        ["git", *args],
        cwd=REPO_DIR,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(f"git {' '.join(args)} failed:\n{result.stderr.strip()}")
    return result.stdout.strip()


# ── bhavcopy index ─────────────────────────────────────────────────────────────
def write_bhav_index():
    """
    Scans the bhavcopy/ folder and writes bhavcopy_index.json.
    Format: { "files": ["sec_bhavdata_full_DDMMYYYY.csv", ...], "count": N, "updated": ISO }
    Dashboard reads this to know which historical sessions are available for backfill.
    Files are listed in chronological order (oldest first).
    """
    os.makedirs(BHAV_DIR, exist_ok=True)
    files = sorted([
        f for f in os.listdir(BHAV_DIR)
        if f.startswith("sec_bhavdata_full_") and f.endswith(".csv")
    ])
    index = {
        "files": files,
        "count": len(files),
        "updated": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    }
    with open(INDEX_JSON, "w", encoding="utf-8") as fh:
        json.dump(index, fh, separators=(",", ":"))
    log_and_print(f"  bhavcopy_index.json: {len(files)} files indexed")


# ── main ───────────────────────────────────────────────────────────────────────
def main():
    # ── parse optional --date DDMMYYYY argument ──────────────────────────────
    today = date.today()
    args = sys.argv[1:]
    if '--date' in args:
        idx = args.index('--date')
        if idx + 1 < len(args):
            raw = args[idx + 1]
            try:
                today = datetime.strptime(raw, '%d%m%Y').date()
            except ValueError:
                print(f"ERROR: --date must be DDMMYYYY, got '{raw}'")
                sys.exit(1)
        else:
            print("ERROR: --date requires a value e.g. --date 26092026")
            sys.exit(1)

    log_and_print(f"=== accradar_sync  {today.isoformat()} ===")

    # ── 1. Bhavcopy ─────────────────────────────────────────────────────────
    bhav_name   = f"sec_bhavdata_full_{today.strftime('%d%m%Y')}.csv"
    bhav_target = os.path.join(BHAV_DIR, bhav_name)
    bhav_csv    = None

    if os.path.exists(bhav_target):
        log_and_print(f"  bhavcopy: already on disk — skipping fetch")
        with open(bhav_target, "rb") as fh:
            bhav_csv = fh.read()
    else:
        bhav_csv = fetch(bhav_url(today), "bhavcopy")
        if bhav_csv is None:
            log_and_print("Bhavcopy not available yet — exiting silently.", "warning")
            sys.exit(0)
        os.makedirs(BHAV_DIR, exist_ok=True)
        with open(bhav_target, "wb") as fh:
            fh.write(bhav_csv)

    # ── 2. Indices ──────────────────────────────────────────────────────────
    idx_name   = f"ind_close_all_{today.strftime('%d%m%Y')}.csv"
    idx_target = os.path.join(REPO_DIR, "indices", idx_name)
    idx_csv    = None

    if os.path.exists(idx_target):
        log_and_print(f"  indices: already on disk — skipping fetch")
        with open(idx_target, "rb") as fh:
            idx_csv = fh.read()
    else:
        idx_csv = fetch(indices_url(today), "indices")
        if idx_csv:
            os.makedirs(os.path.dirname(idx_target), exist_ok=True)
            with open(idx_target, "wb") as fh:
                fh.write(idx_csv)

    # ── 3. 52-week H/L ──────────────────────────────────────────────────────
    wk52_fname  = f"CM_52_wk_High_low_{today.strftime('%d%m%Y')}.csv"
    wk52_target = os.path.join(REPO_DIR, "wk52", wk52_fname)
    wk52_csv    = None

    if os.path.exists(wk52_target):
        log_and_print(f"  wk52: already on disk — skipping fetch")
        with open(wk52_target, "rb") as fh:
            wk52_csv = fh.read()
    else:
        wk52_csv = fetch(wk52_url(today), "wk52")
        if wk52_csv:
            os.makedirs(os.path.dirname(wk52_target), exist_ok=True)
            with open(wk52_target, "wb") as fh:
                fh.write(wk52_csv)

    # ── 4. Bulk deals (overwrite daily) ─────────────────────────────────────
    bulk_target = os.path.join(REPO_DIR, "bulk", "bulk.csv")
    bulk_csv    = fetch(BULK_URL, "bulk deals")
    if bulk_csv:
        os.makedirs(os.path.dirname(bulk_target), exist_ok=True)
        with open(bulk_target, "wb") as fh:
            fh.write(bulk_csv)

    # ── 5. Block deals (overwrite daily) ────────────────────────────────────
    block_target = os.path.join(REPO_DIR, "block", "block.csv")
    block_csv    = fetch(BLOCK_URL, "block deals")
    if block_csv:
        os.makedirs(os.path.dirname(block_target), exist_ok=True)
        with open(block_target, "wb") as fh:
            fh.write(block_csv)

    # ── 6. Write bhavcopy_index.json ────────────────────────────────────────
    write_bhav_index()

    # ── 7. Write dashboard-data.json (today's files only) ───────────────────

    # Extract Nifty 500 closing price from ind_close_all so the dashboard can
    # keep IDX500 current without re-embedding the constant each session.
    nifty500_close = None
    if idx_csv:
        try:
            reader = csv.DictReader(io.StringIO(idx_csv.decode("utf-8", errors="replace")))
            for row in reader:
                if "NIFTY 500" in row.get("Index Name", "").upper():
                    nifty500_close = float(row["Closing Index Value"].strip())
                    break
        except Exception as e:
            log_and_print(f"  idx500 extract: {e}", "warning")

    payload = {
        "generated": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "date": today.isoformat(),
        "files": {}
    }

    # idx500: single-date dict so dashboard can Object.assign(IDX500, payload.idx500)
    if nifty500_close is not None:
        payload["idx500"] = {today.isoformat(): nifty500_close}
        log_and_print(f"  idx500: Nifty 500 close {nifty500_close} for {today.isoformat()}")
    else:
        log_and_print("  idx500: Nifty 500 close not found in ind_close_all", "warning")

    def add_file(name, data):
        if data is not None:
            payload["files"][name] = data.decode("utf-8", errors="replace")

    add_file(bhav_name,  bhav_csv)
    add_file(idx_name,   idx_csv)
    add_file(wk52_fname, wk52_csv)
    if bulk_csv:
        add_file("bulk.csv",  bulk_csv)
    if block_csv:
        add_file("block.csv", block_csv)

    with open(DATA_JSON, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, separators=(",", ":"))

    log_and_print(f"  dashboard-data.json: {len(payload['files'])} files packaged")

    # ── 8. git add -> commit -> pull --rebase -> push ────────────────────────
    # Close log handlers before git ops — Windows locks open files during rebase
    logging.shutdown()
    try:
        git("add", ".")
        status = git("status", "--porcelain")
        if not status:
            log_and_print("  git: nothing to commit (data unchanged)")
            log_and_print("=== done ===\n")
            return

        commit_msg = f"nightly sync {today.isoformat()}"
        git("commit", "-m", commit_msg)

        # Pull with rebase before pushing to handle any remote-ahead divergence.
        # This is the root cause of the recurring "fetch first" rejections seen
        # in the log — manual edits on GitHub web UI leave the remote ahead.
        # --rebase keeps history linear (no merge commits on data files).
        # --autostash protects any accidental uncommitted local changes.
        try:
            git("pull", "--rebase", "--autostash")
        except RuntimeError as pull_exc:
            log_and_print(f"  git: pull --rebase failed — {pull_exc}", "error")
            log_and_print("  git: aborting rebase and exiting", "error")
            try:
                git("rebase", "--abort")
            except RuntimeError:
                pass
            sys.exit(1)

        git("push")
        log_and_print(f"  git: pushed  OK")

    except RuntimeError as exc:
        log_and_print(f"  git error: {exc}", "error")
        sys.exit(1)

    log_and_print("=== done ===\n")


if __name__ == "__main__":
    main()
