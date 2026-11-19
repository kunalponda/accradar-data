#!/usr/bin/env python3
"""
accradar_sync.py  v2.0 -- Accumulation Radar nightly data sync
Lives in (and runs from): C:/AccRadar/accradar-data/

Run via Windows Task Scheduler, daily at 8:05 PM.

What it does, in order:
  1. Git pre-flight (self-healing): stale lock files, stuck rebase/merge/
     cherry-pick/revert, detached HEAD / wrong branch, object corruption,
     leftover modified files. Every repair is logged to accradar.log.
  2. Pull-first: fetch + rebase onto origin/main BEFORE writing anything.
     On a rebase conflict: abort, keep any locally-added data files,
     hard-reset to origin/main, put the data files back.
  3. Tidy: any bhavcopy/indices/wk52 file whose INTERNAL date does not match
     its filename is removed (NSE serves the previous session's bhavcopy for
     weekends/holidays -- v1 saved those under the wrong date).
  4. Catch-up: checks the last CATCHUP_DAYS calendar days (+ today). Any
     trading day missing on disk is fetched. A file is only ever saved under
     the date printed INSIDE it, so closed days are skipped automatically
     (no holiday calendar needed) and a run before NSE publishes never stores
     yesterday's data under today's name.
  5. bulk.csv / block.csv refreshed.
  6. bhavcopy_index.json rebuilt in true date order.
  7. dashboard-data.json rebuilt for the latest trading day (+ idx500 for every
     day in the catch-up window). Only rewritten when its content changes.
  8. git add -A -> commit -> push (retry once after 30s with a fresh rebase).
     Earlier commits that never reached GitHub are pushed too.

Usage:
  python accradar_sync.py                  # normal nightly run (with catch-up)
  python accradar_sync.py --date 26092026  # fetch one specific date (DDMMYYYY)
  python accradar_sync.py --days 30        # wider catch-up window
  python accradar_sync.py --no-git         # data only, no git (testing)

Compatible with Python 3.7+.
"""

import csv
import glob
import io
import json
import logging
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
from datetime import date, datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple

import requests

SCRIPT_VERSION = "2.0"

# -- config ------------------------------------------------------------------
REPO_DIR   = os.path.dirname(os.path.abspath(__file__))   # C:\AccRadar\accradar-data
LOG_FILE   = os.path.join(REPO_DIR, "accradar.log")
DATA_JSON  = os.path.join(REPO_DIR, "dashboard-data.json")
INDEX_JSON = os.path.join(REPO_DIR, "bhavcopy_index.json")
BHAV_DIR   = os.path.join(REPO_DIR, "bhavcopy")
IDX_DIR    = os.path.join(REPO_DIR, "indices")
WK52_DIR   = os.path.join(REPO_DIR, "wk52")
BULK_DIR   = os.path.join(REPO_DIR, "bulk")
BLOCK_DIR  = os.path.join(REPO_DIR, "block")
GIT_DIR    = os.path.join(REPO_DIR, ".git")

REMOTE        = "origin"
BRANCH        = "main"
CATCHUP_DAYS  = 7          # calendar days looked back on every run
NSE_PAUSE_SEC = 1.0        # politeness gap between NSE requests
LOCK_MAX_AGE  = 120        # seconds; older git *.lock files are treated as stale
PUSH_RETRY_WAIT = 30

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

# -- logging -------------------------------------------------------------------
log = logging.getLogger("accradar")


def setup_logging():
    log.setLevel(logging.INFO)
    fh = logging.FileHandler(LOG_FILE, encoding="utf-8")
    fh.setFormatter(logging.Formatter("%(asctime)s  %(levelname)s  %(message)s",
                                      "%Y-%m-%d %H:%M:%S"))
    log.addHandler(fh)


def say(msg, level="info"):
    print(msg)
    getattr(log, level)(msg)


# -- small helpers -------------------------------------------------------------
def ddmmyyyy(d):
    return d.strftime("%d%m%Y")


def bhav_name(d):
    return "sec_bhavdata_full_%s.csv" % ddmmyyyy(d)


def idx_name(d):
    return "ind_close_all_%s.csv" % ddmmyyyy(d)


def wk52_name(d):
    return "CM_52_wk_High_low_%s.csv" % ddmmyyyy(d)


def date_from_fname(fname):
    m = re.search(r"(\d{2})(\d{2})(\d{4})\.csv$", fname)
    if not m:
        return None
    try:
        return date(int(m.group(3)), int(m.group(2)), int(m.group(1)))
    except ValueError:
        return None


def read_head(path, nbytes=4096):
    with open(path, "rb") as fh:
        return fh.read(nbytes)


def read_bytes(path):
    with open(path, "rb") as fh:
        return fh.read()


def write_bytes(path, data):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "wb") as fh:
        fh.write(data)
    os.replace(tmp, path)


# -- internal-date parsers (the core of the "never mislabel" rule) -------------
def bhav_internal_date(content):
    """DATE1 of the first data row of a sec_bhavdata_full file."""
    try:
        text = content.decode("utf-8", errors="replace")
        rdr = csv.reader(io.StringIO(text))
        header = [h.strip().upper() for h in next(rdr)]
        col = header.index("DATE1")
        for row in rdr:
            if len(row) > col and row[col].strip():
                return datetime.strptime(row[col].strip(), "%d-%b-%Y").date()
    except Exception:
        pass
    return None


def idx_internal_date(content):
    """'Index Date' of the first row of an ind_close_all file."""
    try:
        text = content.decode("utf-8", errors="replace")
        rdr = csv.DictReader(io.StringIO(text))
        for row in rdr:
            v = (row.get("Index Date") or "").strip()
            if v:
                return datetime.strptime(v, "%d-%m-%Y").date()
    except Exception:
        pass
    return None


def wk52_internal_date(content):
    """'Effective for DD-Mon-YYYY' line of a CM_52_wk_High_low file."""
    try:
        text = content.decode("utf-8", errors="replace")
        m = re.search(r"Effective for\s+(\d{1,2}-[A-Za-z]{3}-\d{4})", text)
        if m:
            return datetime.strptime(m.group(1), "%d-%b-%Y").date()
    except Exception:
        pass
    return None


def nifty500_close(content):
    """Exact 'Nifty 500' row (not 'Nifty 500 Momentum 50' etc.)."""
    try:
        text = content.decode("utf-8", errors="replace")
        for row in csv.DictReader(io.StringIO(text)):
            if (row.get("Index Name") or "").strip().upper() == "NIFTY 500":
                return float(row["Closing Index Value"].strip())
    except Exception:
        pass
    return None


# -- NSE fetch -----------------------------------------------------------------
_session = None
_last_req = [0.0]


def nse_get(url, label):
    """GET with NSE headers. Returns bytes, or None on 404/503/network error."""
    global _session
    if _session is None:
        _session = requests.Session()
        _session.headers.update(HEADERS)
    wait = NSE_PAUSE_SEC - (time.time() - _last_req[0])
    if wait > 0:
        time.sleep(wait)
    try:
        r = _session.get(url, timeout=30)
        _last_req[0] = time.time()
        if r.status_code == 404:
            say("    %s: not published (404)" % label)
            return None
        if r.status_code == 503:
            say("    %s: NSE busy (503) -- next run will retry" % label)
            return None
        r.raise_for_status()
        return r.content
    except requests.RequestException as exc:
        _last_req[0] = time.time()
        say("    %s: fetch error -- %s" % (label, exc), "warning")
        return None


def url_bhav(d):
    return "https://nsearchives.nseindia.com/products/content/" + bhav_name(d)


def url_idx(d):
    return "https://nsearchives.nseindia.com/content/indices/" + idx_name(d)


def url_wk52(d):
    return "https://nsearchives.nseindia.com/content/" + wk52_name(d)


BULK_URL  = "https://nsearchives.nseindia.com/content/equities/bulk.csv"
BLOCK_URL = "https://nsearchives.nseindia.com/content/equities/block.csv"


# -- git -----------------------------------------------------------------------
def git(*args, check=True):
    """Run git in the repo. check=True raises on failure; else returns (rc, out, err)."""
    r = subprocess.run(["git"] + list(args), cwd=REPO_DIR,
                       capture_output=True, text=True)
    if check:
        if r.returncode != 0:
            raise RuntimeError("git %s failed:\n%s" % (" ".join(args), r.stderr.strip()))
        return r.stdout.strip()
    return r.returncode, r.stdout.strip(), r.stderr.strip()


def git_ok(*args):
    return git(*args, check=False)[0] == 0


def remote_ref():
    return "%s/%s" % (REMOTE, BRANCH)


def has_remote_ref():
    return git_ok("rev-parse", "--verify", "--quiet", remote_ref())


def ensure_gitignore():
    """accradar.log must never be tracked -- Windows locks it while we log."""
    gi = os.path.join(REPO_DIR, ".gitignore")
    lines = []
    if os.path.exists(gi):
        with open(gi, "r", encoding="utf-8", errors="replace") as fh:
            lines = [l.rstrip("\r\n") for l in fh]
    changed = False
    for pat in ("accradar.log", "*.tmp"):
        if pat not in lines:
            lines.append(pat)
            changed = True
    if changed:
        with open(gi, "w", encoding="utf-8") as fh:
            fh.write("\n".join(lines) + "\n")
        say("  heal: .gitignore updated (accradar.log, *.tmp)")
    if git(("ls-files"), "accradar.log", check=False)[1]:
        git_ok("rm", "--cached", "--quiet", "accradar.log")
        say("  heal: accradar.log was tracked -- untracked it")


def preflight():
    """Detect and repair a broken local repo state before touching anything."""
    say("  git pre-flight")
    if not os.path.isdir(GIT_DIR):
        raise RuntimeError("%s is not a git repository" % REPO_DIR)

    # 1. stale lock files (crashed/killed earlier run)
    now = time.time()
    locks = glob.glob(os.path.join(GIT_DIR, "*.lock")) + \
        glob.glob(os.path.join(GIT_DIR, "refs", "**", "*.lock"), recursive=True)
    for lk in locks:
        try:
            age = now - os.path.getmtime(lk)
            if age > LOCK_MAX_AGE:
                os.remove(lk)
                say("  heal: removed stale lock %s (%.0fs old)" % (os.path.relpath(lk, REPO_DIR), age))
            else:
                say("  pre-flight: lock %s is fresh (%.0fs) -- another git may be running"
                    % (os.path.relpath(lk, REPO_DIR), age), "warning")
        except OSError as e:
            say("  heal: could not remove %s -- %s" % (lk, e), "warning")

    # 2. stuck rebase
    if os.path.isdir(os.path.join(GIT_DIR, "rebase-merge")) or \
            os.path.isdir(os.path.join(GIT_DIR, "rebase-apply")):
        if git_ok("rebase", "--abort"):
            say("  heal: aborted a stuck rebase")
        else:
            git_ok("rebase", "--quit")
            for dname in ("rebase-merge", "rebase-apply"):
                shutil.rmtree(os.path.join(GIT_DIR, dname), ignore_errors=True)
            git_ok("reset", "--hard")
            say("  heal: stuck rebase could not abort -- quit + reset --hard")

    # 3. stuck merge / cherry-pick / revert
    for head, cmd in (("MERGE_HEAD", "merge"), ("CHERRY_PICK_HEAD", "cherry-pick"),
                      ("REVERT_HEAD", "revert")):
        if os.path.exists(os.path.join(GIT_DIR, head)):
            if git_ok(cmd, "--abort"):
                say("  heal: aborted a stuck %s" % cmd)
            else:
                if cmd != "merge":
                    git_ok(cmd, "--quit")
                git_ok("reset", "--hard")
                try:
                    os.remove(os.path.join(GIT_DIR, head))
                except OSError:
                    pass
                say("  heal: stuck %s could not abort -- reset --hard" % cmd)

    # 4. detached HEAD / wrong branch
    rc, cur, _ = git("symbolic-ref", "--quiet", "--short", "HEAD", check=False)
    if rc != 0 or cur != BRANCH:
        what = "detached HEAD" if rc != 0 else "on branch '%s'" % cur
        if git_ok("checkout", "-f", BRANCH):
            say("  heal: %s -> checked out %s" % (what, BRANCH))
        elif has_remote_ref() and git_ok("checkout", "-f", "-B", BRANCH, remote_ref()):
            say("  heal: %s -> recreated %s from %s" % (what, BRANCH, remote_ref()))
        else:
            raise RuntimeError("could not get back onto branch %s" % BRANCH)

    # 5. object corruption
    rc, out, err = git("fsck", "--connectivity-only", "--no-progress", check=False)
    err = (err or out)
    if rc != 0:
        say("  heal: fsck reported problems -- running gc --prune=now\n    %s" % err[:300], "warning")
        git_ok("gc", "--prune=now")
        rc2, out2, err2 = git("fsck", "--connectivity-only", "--no-progress", check=False)
        err2 = (err2 or out2)
        if rc2 == 0:
            say("  heal: repository clean after gc")
        else:
            say("  pre-flight: repository still reports corruption after gc -- "
                "continuing, push may fail. Details: %s" % err2[:300], "error")

    # 6. leftover modified tracked files (untracked new data files are kept)
    ensure_gitignore()
    rc, out, _ = git("status", "--porcelain", "--untracked-files=no", check=False)
    modified = [l for l in out.splitlines() if l.strip() and not l.endswith(".gitignore")]
    if modified:
        git_ok("reset", "--quiet")            # unstage anything half-added
        git_ok("checkout", "--", ".")
        say("  heal: discarded %d leftover modified file(s): %s"
            % (len(modified), ", ".join(l.split(None, 1)[-1] for l in modified[:5])))


def recover_by_reset():
    """Rebase conflict: keep locally ADDED files, take remote for everything else."""
    rc, added, _ = git("diff", "--name-only", "--diff-filter=A", remote_ref(), "HEAD", check=False)
    keep = [p for p in added.splitlines() if p.strip()]
    tmpdir = tempfile.mkdtemp(prefix="accradar_keep_")
    try:
        for p in keep:
            src = os.path.join(REPO_DIR, p)
            if os.path.isfile(src):
                dst = os.path.join(tmpdir, p)
                os.makedirs(os.path.dirname(dst), exist_ok=True)
                shutil.copy2(src, dst)
        git("reset", "--hard", remote_ref())
        for p in keep:
            src = os.path.join(tmpdir, p)
            if os.path.isfile(src):
                dst = os.path.join(REPO_DIR, p)
                os.makedirs(os.path.dirname(dst), exist_ok=True)
                shutil.copy2(src, dst)
        say("  heal: conflict -> reset to %s, re-applied %d locally added file(s)"
            % (remote_ref(), len(keep)))
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def pull():
    """Fetch + rebase onto origin/main. Returns False if GitHub is unreachable."""
    rc, _, err = git("fetch", "--quiet", REMOTE, check=False)
    if rc != 0:
        say("  git: fetch failed (offline?) -- continuing locally. %s" % err[:200], "warning")
        return False
    if not has_remote_ref():
        return True
    behind = int(git("rev-list", "--count", "HEAD.." + remote_ref()) or 0)
    ahead = int(git("rev-list", "--count", remote_ref() + "..HEAD") or 0)
    if ahead:
        say("  git: %d earlier commit(s) not yet on GitHub -- will be pushed today" % ahead)
    if behind:
        if git_ok("rebase", "--quiet", remote_ref()):
            say("  git: pulled %d new commit(s) from GitHub" % behind)
        else:
            git_ok("rebase", "--abort")
            recover_by_reset()
    return True


def commit_and_push(label):
    git("add", "-A")
    rc, status, _ = git("status", "--porcelain", check=False)
    if status:
        git("commit", "--quiet", "-m", "nightly sync %s" % label)
        say("  git: committed")
    else:
        say("  git: nothing new to commit")
    if not has_remote_ref():
        git_ok("fetch", "--quiet", REMOTE)
    if has_remote_ref():
        rc, out, _ = git("rev-list", "--count", remote_ref() + "..HEAD", check=False)
        ahead = int(out) if rc == 0 and out.isdigit() else 1
    else:
        ahead = 1
    if not ahead:
        return True
    for attempt in (1, 2):
        rc, _, err = git("push", REMOTE, BRANCH, check=False)
        if rc == 0:
            say("  git: pushed %d commit(s)  OK" % ahead)
            return True
        say("  git: push attempt %d failed -- %s" % (attempt, err[:300]), "warning")
        if attempt == 1:
            time.sleep(PUSH_RETRY_WAIT)
            pull()
    say("  git: push failed twice -- commits stay local and go out on the next run", "error")
    return False


# -- data steps ----------------------------------------------------------------
def tidy_mislabelled():
    """Delete files whose internal date != filename date (v1 weekend/holiday copies)."""
    removed = 0
    for folder, parser in ((BHAV_DIR, bhav_internal_date),
                           (IDX_DIR, idx_internal_date),
                           (WK52_DIR, wk52_internal_date)):
        if not os.path.isdir(folder):
            continue
        for fname in sorted(os.listdir(folder)):
            if not fname.endswith(".csv"):
                continue
            fdate = date_from_fname(fname)
            path = os.path.join(folder, fname)
            head = read_head(path, 4096 if parser is not wk52_internal_date else 2048)
            idate = parser(head)
            if fdate is None or idate is None:
                continue          # unreadable -> leave it alone, never guess
            if idate != fdate:
                os.remove(path)
                removed += 1
                say("  tidy: removed %s/%s (contains %s data)"
                    % (os.path.basename(folder), fname, idate.isoformat()))
    if removed == 0:
        say("  tidy: all files carry the right date")
    return removed


def ensure_file(d, folder, namefn, urlfn, parser, label):
    """Make sure a correctly-dated file for day d is on disk. Returns bytes or None."""
    path = os.path.join(folder, namefn(d))
    if os.path.exists(path):
        data = read_bytes(path)
        if parser(data) == d:
            return data
        os.remove(path)
        say("    %s: on-disk copy had the wrong date -- refetching" % label)
    data = nse_get(urlfn(d), label)
    if data is None:
        return None
    idate = parser(data)
    if idate != d:
        say("    %s: NSE served %s data for %s -- not saved"
            % (label, idate.isoformat() if idate else "unreadable", d.isoformat()))
        return None
    write_bytes(path, data)
    say("    %s: saved (%s bytes)" % (label, format(len(data), ",")))
    return data


def process_day(d):
    """Returns True if d is a trading day we now hold a bhavcopy for."""
    say("  %s %s" % (d.isoformat(), d.strftime("%a")))
    had = os.path.exists(os.path.join(BHAV_DIR, bhav_name(d)))
    bhav = ensure_file(d, BHAV_DIR, bhav_name, url_bhav, bhav_internal_date, "bhavcopy")
    if bhav is None:
        return False
    if not had:
        NEWLY_FETCHED.append(d)
    ensure_file(d, IDX_DIR, idx_name, url_idx, idx_internal_date, "indices")
    ensure_file(d, WK52_DIR, wk52_name, url_wk52, wk52_internal_date, "wk52")
    return True


NEWLY_FETCHED = []   # type: List[date]


def refresh_deals():
    for url, folder, fname, label in ((BULK_URL, BULK_DIR, "bulk.csv", "bulk deals"),
                                      (BLOCK_URL, BLOCK_DIR, "block.csv", "block deals")):
        data = nse_get(url, label)
        if data:
            path = os.path.join(folder, fname)
            if not (os.path.exists(path) and read_bytes(path) == data):
                write_bytes(path, data)
                say("    %s: updated" % label)


def bhav_dates_on_disk():
    out = []
    if os.path.isdir(BHAV_DIR):
        for f in os.listdir(BHAV_DIR):
            if f.startswith("sec_bhavdata_full_") and f.endswith(".csv"):
                d = date_from_fname(f)
                if d:
                    out.append(d)
    return sorted(out)


def write_bhav_index():
    """bhavcopy_index.json -- files in TRUE chronological order (v1 sorted by DDMM)."""
    dates = bhav_dates_on_disk()
    files = [bhav_name(d) for d in dates]
    old = None
    if os.path.exists(INDEX_JSON):
        try:
            with open(INDEX_JSON, "r", encoding="utf-8") as fh:
                old = json.load(fh)
        except Exception:
            old = None
    if old and old.get("files") == files:
        say("  bhavcopy_index.json: unchanged (%d files)" % len(files))
        return
    index = {"files": files, "count": len(files),
             "updated": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")}
    with open(INDEX_JSON, "w", encoding="utf-8") as fh:
        json.dump(index, fh, separators=(",", ":"))
    say("  bhavcopy_index.json: %d files indexed (date order)" % len(files))


def write_dashboard_json(window_days):
    dates = bhav_dates_on_disk()
    if not dates:
        say("  dashboard-data.json: no bhavcopy on disk -- skipped", "warning")
        return None
    latest = dates[-1]

    payload = {"generated": None, "date": latest.isoformat(), "files": {},
               "syncVersion": SCRIPT_VERSION}

    def add(path, name):
        if os.path.exists(path):
            payload["files"][name] = read_bytes(path).decode("utf-8", errors="replace")

    add(os.path.join(BHAV_DIR, bhav_name(latest)), bhav_name(latest))
    add(os.path.join(IDX_DIR, idx_name(latest)), idx_name(latest))
    add(os.path.join(WK52_DIR, wk52_name(latest)), wk52_name(latest))
    add(os.path.join(BULK_DIR, "bulk.csv"), "bulk.csv")
    add(os.path.join(BLOCK_DIR, "block.csv"), "block.csv")

    # idx500 for every trading day in the window, keyed by the file's OWN date
    idx500 = {}
    for d in dates:
        if d < latest - timedelta(days=window_days):
            continue
        p = os.path.join(IDX_DIR, idx_name(d))
        if os.path.exists(p):
            data = read_bytes(p)
            idate = idx_internal_date(data)
            c = nifty500_close(data)
            if idate == d and c is not None:
                idx500[d.isoformat()] = c
    if idx500:
        payload["idx500"] = idx500
    # trading days inside the window, so the dashboard can see what it should hold
    payload["recentDates"] = [d.isoformat() for d in dates
                              if d >= latest - timedelta(days=window_days)]

    old = None
    if os.path.exists(DATA_JSON):
        try:
            with open(DATA_JSON, "r", encoding="utf-8") as fh:
                old = json.load(fh)
        except Exception:
            old = None
    if old is not None:
        cmp_old = dict(old)
        cmp_old["generated"] = None
        if cmp_old == payload:
            say("  dashboard-data.json: unchanged (latest %s)" % latest.isoformat())
            return latest
    payload["generated"] = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    with open(DATA_JSON, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, separators=(",", ":"))
    say("  dashboard-data.json: %d files for %s, idx500 x%d"
        % (len(payload["files"]), latest.isoformat(), len(idx500)))
    return latest


# -- main ----------------------------------------------------------------------
def parse_args(argv):
    opts = {"date": None, "days": CATCHUP_DAYS, "git": True}
    i = 0
    while i < len(argv):
        a = argv[i]
        if a == "--date":
            if i + 1 >= len(argv):
                print("ERROR: --date requires a value e.g. --date 26092026")
                sys.exit(1)
            try:
                opts["date"] = datetime.strptime(argv[i + 1], "%d%m%Y").date()
            except ValueError:
                print("ERROR: --date must be DDMMYYYY, got '%s'" % argv[i + 1])
                sys.exit(1)
            i += 2
            continue
        if a == "--days":
            try:
                opts["days"] = max(0, int(argv[i + 1]))
            except (IndexError, ValueError):
                print("ERROR: --days needs a number, e.g. --days 30")
                sys.exit(1)
            i += 2
            continue
        if a == "--no-git":
            opts["git"] = False
            i += 1
            continue
        print("ERROR: unknown argument '%s'" % a)
        sys.exit(1)
    return opts


def main(argv=None):
    opts = parse_args(sys.argv[1:] if argv is None else argv)
    setup_logging()
    today = date.today()
    say("=== accradar_sync v%s  %s ===" % (SCRIPT_VERSION, datetime.now().strftime("%Y-%m-%d %H:%M")))
    exit_code = 0
    try:
        online = True
        if opts["git"]:
            preflight()
            online = pull()

        tidy_mislabelled()

        if opts["date"]:
            days = [opts["date"]]
        else:
            days = [today - timedelta(days=i) for i in range(opts["days"], -1, -1)]
        say("  checking %d day(s): %s -> %s" % (len(days), days[0].isoformat(), days[-1].isoformat()))
        for d in days:
            if d > today:
                continue
            process_day(d)
        if NEWLY_FETCHED:
            say("  new trading days saved: %s" % ", ".join(d.isoformat() for d in NEWLY_FETCHED))
        else:
            say("  no new trading days (already current, weekend, holiday, or not yet published)")

        refresh_deals()
        write_bhav_index()
        latest = write_dashboard_json(max(opts["days"], 7))

        if opts["git"]:
            label = latest.isoformat() if latest else today.isoformat()
            if not commit_and_push(label):
                exit_code = 1
            elif not online:
                say("  note: GitHub was unreachable at the start of this run", "warning")
    except Exception as exc:
        say("  FATAL: %s" % exc, "error")
        exit_code = 1
    say("=== done (exit %d) ===\n" % exit_code)
    logging.shutdown()
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
