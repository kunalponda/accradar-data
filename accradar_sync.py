#!/usr/bin/env python3
"""
accradar_sync.py — Accumulation Radar nightly data sync
Saves to: C:/AccRadar/accradar-data/

Run via Windows Task Scheduler, daily at 8:05 PM.
Fetches 5 NSE files -> saves them to the repo -> writes dashboard-data.json
and bhavcopy_index.json -> git push.

Also regenerates seed.json.gz every 20 sessions (or when forced with --reseed).
seed.json.gz and mahist.json.gz are fetched by the dashboard on first open
instead of being baked into the HTML (v6.35+).

Usage:
  python accradar_sync.py                  # fetch today's files
  python accradar_sync.py --date 26092026  # fetch a specific date (DDMMYYYY)
  python accradar_sync.py --reseed         # force regenerate seed.json.gz now
"""

import os
import sys
import json
import gzip
import subprocess
import logging
from datetime import datetime, date, timezone

import requests

# ── config ─────────────────────────────────────────────────────────────────────
REPO_DIR    = os.path.dirname(os.path.abspath(__file__))
LOG_FILE    = os.path.join(REPO_DIR, "accradar.log")
DATA_JSON   = os.path.join(REPO_DIR, "dashboard-data.json")
INDEX_JSON  = os.path.join(REPO_DIR, "bhavcopy_index.json")
BHAV_DIR    = os.path.join(REPO_DIR, "bhavcopy")
SEED_GZ     = os.path.join(REPO_DIR, "seed.json.gz")
SEED_META   = os.path.join(REPO_DIR, "seed_meta.json")   # tracks last-seeded count

# Regenerate seed every N new bhavcopy sessions added since last seed build.
# 20 sessions ≈ 1 calendar month of trading days.
SEED_REFRESH_INTERVAL = 20

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
    Format: { "files": [...], "count": N, "updated": ISO }
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
    return len(files)


# ── seed builder ───────────────────────────────────────────────────────────────
def _parse_bhav_csv(raw: bytes) -> dict:
    """
    Parse sec_bhavdata_full CSV bytes into {SYM: [vol, deliv_pct, close, pclose, open, high, low, 0, 0]}
    Columns: SYMBOL,SERIES,OPEN,HIGH,LOW,CLOSE,LAST,PREVCLOSE,TOTQTY,TOTVAL,TIMESTAMP,TRADES,ISIN,DELIV_QTY,DELIV_PCT
    Indices:  0      1      2    3    4   5     6    7         8      9      10        11     12   13        14
    """
    day = {}
    try:
        text = raw.decode("utf-8", errors="replace")
        lines = text.splitlines()
        if not lines:
            return day
        # Skip header
        for line in lines[1:]:
            parts = line.split(",")
            if len(parts) < 15:
                continue
            sym    = parts[0].strip()
            series = parts[1].strip()
            if series != "EQ":
                continue
            try:
                vol    = int(parts[8])   if parts[8].strip()  else 0
                close  = float(parts[5]) if parts[5].strip()  else None
                pclose = float(parts[7]) if parts[7].strip()  else None
                open_  = float(parts[2]) if parts[2].strip()  else None
                high   = float(parts[3]) if parts[3].strip()  else None
                low    = float(parts[4]) if parts[4].strip()  else None
                dq     = parts[13].strip()
                dp     = float(parts[14]) if parts[14].strip() else None
                nt     = int(parts[11])   if parts[11].strip() else 0
                if dp is None and dq and vol:
                    try:
                        dp = float(dq) / vol * 100
                    except (ValueError, ZeroDivisionError):
                        dp = None
                # Format: [vol, deliv_pct, close, pclose, open, high, low, 0, 0]
                # Matches rec() field map in dashboard: [0]=vol,[1]=deliv%,[2]=close,[3]=open,[5]=high,[6]=low
                day[sym] = [vol, dp, close, pclose, open_, high, low, nt]
            except (ValueError, IndexError):
                continue
    except Exception:
        pass
    return day


def build_seed_gz(force: bool = False) -> bool:
    """
    Build seed.json.gz from all bhavcopy CSVs on disk.
    Only rebuilds if session count has grown by SEED_REFRESH_INTERVAL since last build.
    Returns True if seed was rebuilt.
    """
    files = sorted([
        f for f in os.listdir(BHAV_DIR)
        if f.startswith("sec_bhavdata_full_") and f.endswith(".csv")
    ])
    n = len(files)

    # Read last-seeded count
    last_seeded = 0
    if os.path.exists(SEED_META):
        try:
            with open(SEED_META) as fh:
                last_seeded = json.load(fh).get("seeded_count", 0)
        except Exception:
            pass

    if not force and (n - last_seeded) < SEED_REFRESH_INTERVAL:
        log_and_print(
            f"  seed.json.gz: {n - last_seeded} new sessions since last build "
            f"(threshold {SEED_REFRESH_INTERVAL}) — skipping"
        )
        return False

    log_and_print(f"  seed.json.gz: rebuilding from {n} sessions…")

    # Build compact dataset: {date: {sym: [fields...]}}
    # Use symbol index for compression: syms list + integer indices in data
    sym_index = {}   # sym -> int
    syms_list = []
    dataset = {}     # date_str -> flat array [symIdx, f0, f1, ..., f8, symIdx, ...]

    for fname in files:
        # Extract date from filename: sec_bhavdata_full_DDMMYYYY.csv
        try:
            ddmmyyyy = fname.replace("sec_bhavdata_full_", "").replace(".csv", "")
            dt = datetime.strptime(ddmmyyyy, "%d%m%Y")
            date_str = dt.strftime("%Y-%m-%d")
        except ValueError:
            continue

        fpath = os.path.join(BHAV_DIR, fname)
        try:
            with open(fpath, "rb") as fh:
                raw = fh.read()
        except OSError:
            continue

        day = _parse_bhav_csv(raw)
        if not day:
            continue

        flat = []
        for sym, fields in day.items():
            if sym not in sym_index:
                sym_index[sym] = len(syms_list)
                syms_list.append(sym)
            flat.append(sym_index[sym])
            flat.extend(fields)
        dataset[date_str] = flat

    if not dataset:
        log_and_print("  seed.json.gz: no data parsed — skipping", "warning")
        return False

    payload = {"syms": syms_list, "d": dataset}
    json_bytes = json.dumps(payload, separators=(",", ":")).encode("utf-8")
    gz_bytes = gzip.compress(json_bytes, compresslevel=9)

    with open(SEED_GZ, "wb") as fh:
        fh.write(gz_bytes)

    # Save metadata
    with open(SEED_META, "w") as fh:
        json.dump({
            "seeded_count": n,
            "built_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "sessions": n,
            "symbols": len(syms_list),
            "gz_bytes": len(gz_bytes),
        }, fh, indent=2)

    log_and_print(
        f"  seed.json.gz: {n} sessions, {len(syms_list)} symbols → "
        f"{len(gz_bytes)/1024:.0f} KB  OK"
    )
    return True


# ── main ───────────────────────────────────────────────────────────────────────
def main():
    # ── parse arguments ──────────────────────────────────────────────────────
    today   = date.today()
    args    = sys.argv[1:]
    reseed  = "--reseed" in args

    if "--date" in args:
        idx = args.index("--date")
        if idx + 1 < len(args):
            raw = args[idx + 1]
            try:
                today = datetime.strptime(raw, "%d%m%Y").date()
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

    # ── 7. Rebuild seed.json.gz if due (every 20 sessions) ──────────────────
    try:
        build_seed_gz(force=reseed)
    except Exception as exc:
        log_and_print(f"  seed.json.gz: build error — {exc}", "warning")

    # ── 8. Write dashboard-data.json (today's files only) ───────────────────
    payload = {
        "generated": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "date": today.isoformat(),
        "files": {}
    }

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

    # ── 9. git add -> commit -> pull --rebase -> push ────────────────────────
    try:
        git("add", ".")
        status = git("status", "--porcelain")
        if not status:
            log_and_print("  git: nothing to commit (data unchanged)")
            log_and_print("=== done ===\n")
            return

        commit_msg = f"nightly sync {today.isoformat()}"
        git("commit", "-m", commit_msg)

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
