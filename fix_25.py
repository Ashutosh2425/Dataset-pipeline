"""
fix_25.py
---------
Widens date ranges for the 25 AOIs that had < 3 usable epochs,
clears their epoch + stack cache, re-queries STAC, re-downloads,
re-stacks, verifies, and pushes to GitHub.

Run: conda run -n tdrd python fix_25.py
"""

import json
import os
import asyncio
import subprocess
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

import httpx

FAILING = [
    'EVT002_0069','EVT002_0070','EVT002_0071','EVT002_0072','EVT002_0073',
    'EVT002_0074','EVT002_0075','EVT002_0077','EVT002_0078','EVT002_0079',
    'EVT002_0080','EVT002_0081','EVT002_0082','EVT002_0083','EVT002_0084',
    'EVT002_0087','EVT002_0088','EVT002_0089','EVT002_0093','EVT002_0103',
    'EVT002_0104','EVT002_0118','EVT012_0576','EVT012_0578','EVT012_0579',
]

AOI_LIST_PATH  = Path("data/aoi_list.json")
EPOCHS_PATH    = Path("data/aoi_epochs.json")
RAW_DIR        = Path("data/raw_scenes")
STACKS_DIR     = Path("data/stacks")
STAC_API_URL   = "https://earth-search.aws.element84.com/v1/search"
LOG            = Path("fix_25.log")

# Widen by this many days on each side
EXTEND_PRE_DAYS  = 90
EXTEND_POST_DAYS = 90
# Relax cloud cover from 25% to 50%
CLOUD_LIMIT = 50


def log(msg):
    line = f"[{time.strftime('%H:%M:%S')}] {msg}"
    print(line, flush=True)
    with open(LOG, "a") as f:
        f.write(line + "\n")


# ── Step A: widen date ranges in aoi_list.json ────────────────────────────────

def widen_date_ranges():
    log("Widening date ranges for 25 failing AOIs...")
    with open(AOI_LIST_PATH) as f:
        aois = json.load(f)

    for aoi in aois:
        if aoi['aoi_id'] not in FAILING:
            continue
        start, end = aoi['date_range']
        new_start = (datetime.strptime(start, '%Y-%m-%d') - timedelta(days=EXTEND_PRE_DAYS)).strftime('%Y-%m-%d')
        new_end   = (datetime.strptime(end,   '%Y-%m-%d') + timedelta(days=EXTEND_POST_DAYS)).strftime('%Y-%m-%d')
        log(f"  {aoi['aoi_id']}: {start}~{end}  →  {new_start}~{new_end}")
        aoi['date_range'] = [new_start, new_end]

    with open(AOI_LIST_PATH, 'w') as f:
        json.dump(aois, f, indent=2)
    log("aoi_list.json updated.")


# ── Step B: clear epoch cache for the 25 AOIs ─────────────────────────────────

def clear_epoch_cache():
    log("Clearing epoch cache for 25 failing AOIs...")
    with open(EPOCHS_PATH) as f:
        epochs = json.load(f)
    for aoi_id in FAILING:
        epochs.pop(aoi_id, None)
    with open(EPOCHS_PATH, 'w') as f:
        json.dump(epochs, f, indent=2)
    log("aoi_epochs.json cleared for failing AOIs.")


# ── Step C: re-query STAC with wider range + relaxed cloud ────────────────────

def _sub_days(date_str, days):
    dt = datetime.strptime(date_str, '%Y-%m-%d')
    return (dt - timedelta(days=days)).strftime('%Y-%m-%d')

def _scene_date(item):
    raw = item['properties'].get('datetime', '') or ''
    return raw[:10].replace('-', '') if len(raw) >= 10 else '00000000'

def _scene_datetime(item):
    d = _scene_date(item)
    try:
        return datetime.strptime(d, '%Y%m%d')
    except ValueError:
        return datetime.min

def _classify(item, start_dt, end_dt):
    dt = _scene_datetime(item)
    if dt < start_dt:
        return 'pre'
    if dt > end_dt:
        return 'post'
    return 'during'

S2_BAND_MAP = {'blue': 'B02', 'green': 'B03', 'red': 'B04', 'nir': 'B08'}

def _build_record(item, sensor, label):
    assets = {}
    raw = item.get('assets', {})
    if sensor == 'S2':
        for k, v in S2_BAND_MAP.items():
            if k in raw and raw[k].get('href'):
                assets[v] = raw[k]['href']
    else:
        for pol in ['vv', 'vh']:
            if pol in raw and raw[pol].get('href'):
                assets[pol.upper()] = raw[pol]['href']
    return {'epoch_label': label, 'date': _scene_date(item),
            'sensor': sensor, 'scene_id': item.get('id',''),
            'cloud_cover': item['properties'].get('eo:cloud_cover'),
            'assets': assets}

def select_epochs(s2_items, s1_items, event_start, event_end, max_epochs=5):
    start_dt = datetime.strptime(event_start, '%Y-%m-%d')
    end_dt   = datetime.strptime(event_end,   '%Y-%m-%d')
    s2_items = sorted(s2_items, key=_scene_datetime)
    s1_items = sorted(s1_items, key=_scene_datetime)
    epochs = []

    pre_s2 = [i for i in s2_items if _classify(i, start_dt, end_dt) == 'pre']
    pre_s1 = [i for i in s1_items if _classify(i, start_dt, end_dt) == 'pre']
    if pre_s2:
        epochs.append(_build_record(min(pre_s2, key=lambda x: x['properties'].get('eo:cloud_cover', 100)), 'S2', 'pre_event'))
    elif pre_s1:
        epochs.append(_build_record(pre_s1[-1], 'S1', 'pre_event'))

    during_s2 = [i for i in s2_items if _classify(i, start_dt, end_dt) == 'during']
    during_s1 = [i for i in s1_items if _classify(i, start_dt, end_dt) == 'during']
    n_slots = min(3, max_epochs - len(epochs) - 1)
    pool = during_s2 if during_s2 else during_s1
    sensor = 'S2' if during_s2 else 'S1'
    if pool and n_slots > 0:
        n = len(pool)
        idxs = list(range(n)) if n <= n_slots else sorted(set(
            [round(i*(n-1)/(n_slots-1)) for i in range(n_slots)] if n_slots > 1 else [n//2]
        ))
        for si, pi in enumerate(idxs[:n_slots]):
            epochs.append(_build_record(pool[pi], sensor, f'during_{si+1}'))

    post_s2 = [i for i in s2_items if _classify(i, start_dt, end_dt) == 'post']
    post_s1 = [i for i in s1_items if _classify(i, start_dt, end_dt) == 'post']
    if len(epochs) < max_epochs:
        if post_s2:
            epochs.append(_build_record(min(post_s2, key=lambda x: x['properties'].get('eo:cloud_cover', 100)), 'S2', 'post_event'))
        elif post_s1:
            epochs.append(_build_record(post_s1[0], 'S1', 'post_event'))

    epochs.sort(key=lambda e: e['date'])
    return epochs


async def _query_one(client, aoi, sem):
    bbox = aoi['bbox']
    event_start, event_end = aoi['date_range']
    pre_start    = _sub_days(event_start, 30)
    datetime_str = f"{pre_start}T00:00:00Z/{event_end}T23:59:59Z"

    async with sem:
        try:
            s2_resp = await client.post(STAC_API_URL, json={
                "collections": ["sentinel-2-l2a"], "bbox": bbox,
                "datetime": datetime_str, "limit": 100,
                "query": {"eo:cloud_cover": {"lt": CLOUD_LIMIT}},
            }, timeout=60.0)
            s2_items = s2_resp.json().get('features', []) if s2_resp.status_code == 200 else []

            s1_resp = await client.post(STAC_API_URL, json={
                "collections": ["sentinel-1-grd"], "bbox": bbox,
                "datetime": datetime_str, "limit": 100,
            }, timeout=60.0)
            s1_items = s1_resp.json().get('features', []) if s1_resp.status_code == 200 else []

            epochs = select_epochs(s2_items, s1_items, event_start, event_end)
            return aoi['aoi_id'], epochs
        except Exception as e:
            log(f"  QUERY ERROR {aoi['aoi_id']}: {e}")
            return aoi['aoi_id'], []


async def _run_queries(aois_to_query):
    sem    = asyncio.Semaphore(10)
    limits = httpx.Limits(max_connections=20, max_keepalive_connections=10)
    results = {}
    async with httpx.AsyncClient(limits=limits) as client:
        tasks = [_query_one(client, aoi, sem) for aoi in aois_to_query]
        for coro in asyncio.as_completed(tasks):
            aoi_id, epochs = await coro
            results[aoi_id] = epochs
    return results


def requery_epochs():
    log(f"Re-querying STAC for 25 AOIs (cloud<{CLOUD_LIMIT}%, ±{EXTEND_PRE_DAYS}d wider)...")
    with open(AOI_LIST_PATH) as f:
        all_aois = json.load(f)
    failing_aois = [a for a in all_aois if a['aoi_id'] in FAILING]

    if os.name == 'nt':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    new_epochs = asyncio.run(_run_queries(failing_aois))

    with open(EPOCHS_PATH) as f:
        existing = json.load(f)
    existing.update(new_epochs)
    with open(EPOCHS_PATH, 'w') as f:
        json.dump(existing, f, indent=2)

    good = sum(1 for aoi_id in FAILING if len(new_epochs.get(aoi_id, [])) >= 3)
    log(f"Re-query done: {good}/25 AOIs now have >= 3 epochs.")
    for aoi_id in FAILING:
        n = len(new_epochs.get(aoi_id, []))
        if n < 3:
            log(f"  STILL LOW: {aoi_id} → {n} epochs")
    return good


# ── Step D: clear raw_scenes flags + stacks for the 25 ───────────────────────

def clear_raw_and_stacks():
    log("Clearing download flags and stacks for 25 AOIs so they re-download/re-stack...")
    for aoi_id in FAILING:
        flag = RAW_DIR / aoi_id / "download_complete.flag"
        if flag.exists():
            flag.unlink()
        stack = STACKS_DIR / f"{aoi_id}_stack.tif"
        meta  = STACKS_DIR / f"{aoi_id}_meta.json"
        for f in [stack, meta]:
            if f.exists():
                f.unlink()
    log("Cleared.")


# ── Step E: run pipeline steps ────────────────────────────────────────────────

def run_cmd(label, args):
    log(f"Running: {label}...")
    result = subprocess.run(
        [sys.executable] + args,
        capture_output=True, text=True
    )
    out = (result.stdout or '') + (result.stderr or '')
    log(out[-2000:] if out else "(no output)")
    log(f"{label} exit code: {result.returncode}")
    return result.returncode == 0


# ── Step F: push to GitHub ────────────────────────────────────────────────────

def push_github():
    log("Pushing to GitHub...")
    for cmd in [
        ["git", "add", "data/aoi_list.json", "data/aoi_epochs.json",
         "data/stacks/", "fix_25.log"],
        ["git", "commit", "-m", "Fix 25 low-epoch AOIs: wider date range + relaxed cloud filter"],
        ["git", "push", "origin", "main"],
    ]:
        r = subprocess.run(cmd, capture_output=True, text=True)
        log(f"  {cmd[1]}: {(r.stdout + r.stderr).strip()[:300]}")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    log("=== fix_25.py started ===")

    widen_date_ranges()
    clear_epoch_cache()
    good = requery_epochs()

    if good == 0:
        log("ERROR: No AOIs recovered from re-query. Stopping.")
        return

    clear_raw_and_stacks()
    run_cmd("step2a-download", ["main.py", "run-step2a-download"])
    run_cmd("step2b-coregister", ["main.py", "run-step2b"])
    run_cmd("verify", ["verify_stacks.py"])
    push_github()

    log("=== fix_25.py done ===")


if __name__ == "__main__":
    main()
