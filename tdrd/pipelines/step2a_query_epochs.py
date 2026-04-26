"""
Step 2A — Epoch Query
Queries Element84 STAC for 3-5 temporal epochs per AOI (pre/during/post event).
Saves results to data/aoi_epochs.json. No credentials required.
"""

import json
import os
import asyncio
import httpx
from pathlib import Path
from datetime import datetime, timedelta

from tdrd.config import STAC_API_URL, AOI_LIST_PATH

EPOCHS_PATH = "data/aoi_epochs.json"

# Element84 STAC asset keys → our band names
S2_BAND_MAP = {
    'blue':  'B02',   # 10m
    'green': 'B03',   # 10m
    'red':   'B04',   # 10m
    'nir':   'B08',   # 10m
}
S1_POL_KEYS = ['vv', 'vh']   # IW GRD polarisations


def _sub_days(date_str: str, days: int) -> str:
    dt = datetime.strptime(date_str, '%Y-%m-%d')
    return (dt - timedelta(days=days)).strftime('%Y-%m-%d')


def _scene_date(item: dict) -> str:
    """Return YYYYMMDD string from a STAC item."""
    raw = item['properties'].get('datetime', '') or ''
    return raw[:10].replace('-', '') if len(raw) >= 10 else '00000000'


def _scene_datetime(item: dict) -> datetime:
    d = _scene_date(item)
    try:
        return datetime.strptime(d, '%Y%m%d')
    except ValueError:
        return datetime.min


def _classify(item: dict, start_dt: datetime, end_dt: datetime) -> str:
    dt = _scene_datetime(item)
    if dt < start_dt:
        return 'pre'
    if dt > end_dt:
        return 'post'
    return 'during'


def _build_epoch_record(item: dict, sensor: str, label: str) -> dict:
    """Convert a STAC feature into an epoch dict for aoi_epochs.json."""
    assets = {}
    raw_assets = item.get('assets', {})

    if sensor == 'S2':
        for stac_key, band_name in S2_BAND_MAP.items():
            if stac_key in raw_assets:
                href = raw_assets[stac_key].get('href', '')
                if href:
                    assets[band_name] = href
    else:  # S1
        for pol in S1_POL_KEYS:
            if pol in raw_assets:
                href = raw_assets[pol].get('href', '')
                if href:
                    assets[pol.upper()] = href

    return {
        'epoch_label': label,
        'date':        _scene_date(item),
        'sensor':      sensor,
        'scene_id':    item.get('id', ''),
        'cloud_cover': item['properties'].get('eo:cloud_cover', None),
        'assets':      assets,
    }


def _dominant_tile(items: list) -> str:
    """Returns the tile ID that appears most frequently across a list of STAC items."""
    counts: dict = {}
    for item in items:
        tile = item.get('id', '').split('_')[1] if item.get('id', '') else ''
        if tile:
            counts[tile] = counts.get(tile, 0) + 1
    return max(counts, key=counts.get) if counts else ''


def _filter_to_tile(items: list, tile: str) -> list:
    """Keeps only items whose scene_id belongs to the given tile."""
    return [i for i in items if i.get('id', '').split('_')[1] == tile]


def select_epochs(s2_items: list, s1_items: list,
                  event_start: str, event_end: str,
                  max_epochs: int = 5) -> list:
    """Select 3-5 temporally spread epochs from a single consistent tile.
    Returns [] if coverage < 3 epochs."""
    start_dt = datetime.strptime(event_start, '%Y-%m-%d')
    end_dt   = datetime.strptime(event_end,   '%Y-%m-%d')

    # Enforce tile consistency: all S2 epochs must come from the same tile.
    # Pick the tile with the most scenes (best coverage of this AOI).
    if s2_items:
        best_tile = _dominant_tile(s2_items)
        s2_items  = _filter_to_tile(s2_items, best_tile)

    # Sort all scenes by date
    s2_items = sorted(s2_items, key=_scene_datetime)
    s1_items = sorted(s1_items, key=_scene_datetime)

    epochs = []

    # 1. Pre-event (1 slot)
    pre_s2 = [i for i in s2_items if _classify(i, start_dt, end_dt) == 'pre']
    pre_s1 = [i for i in s1_items if _classify(i, start_dt, end_dt) == 'pre']

    if pre_s2:
        # Prefer lowest cloud cover among pre-event S2 scenes
        best_pre = min(pre_s2, key=lambda x: x['properties'].get('eo:cloud_cover', 100))
        epochs.append(_build_epoch_record(best_pre, 'S2', 'pre_event'))
    elif pre_s1:
        epochs.append(_build_epoch_record(pre_s1[-1], 'S1', 'pre_event'))

    # 2. During-event (2-3 slots)
    during_s2 = [i for i in s2_items if _classify(i, start_dt, end_dt) == 'during']
    during_s1 = [i for i in s1_items if _classify(i, start_dt, end_dt) == 'during']

    # Reserve 1 slot for post-event, fill the rest with during scenes
    n_during_slots = min(3, max_epochs - len(epochs) - 1)

    pool = during_s2 if during_s2 else during_s1
    sensor = 'S2' if during_s2 else 'S1'

    if pool and n_during_slots > 0:
        n = len(pool)
        if n <= n_during_slots:
            chosen_indices = list(range(n))
        else:
            # Evenly spread across the pool
            chosen_indices = [
                round(i * (n - 1) / (n_during_slots - 1))
                for i in range(n_during_slots)
            ] if n_during_slots > 1 else [n // 2]
            chosen_indices = sorted(set(chosen_indices))

        for slot_idx, pool_idx in enumerate(chosen_indices[:n_during_slots]):
            label = f'during_{slot_idx + 1}'
            epochs.append(_build_epoch_record(pool[pool_idx], sensor, label))

    # 3. Post-event (1 slot, optional)
    post_s2 = [i for i in s2_items if _classify(i, start_dt, end_dt) == 'post']
    post_s1 = [i for i in s1_items if _classify(i, start_dt, end_dt) == 'post']

    if len(epochs) < max_epochs:
        if post_s2:
            best_post = min(post_s2, key=lambda x: x['properties'].get('eo:cloud_cover', 100))
            epochs.append(_build_epoch_record(best_post, 'S2', 'post_event'))
        elif post_s1:
            epochs.append(_build_epoch_record(post_s1[0], 'S1', 'post_event'))

    # Sort final list by date for deterministic ordering
    epochs.sort(key=lambda e: e['date'])
    return epochs


async def _query_one_aoi(client: httpx.AsyncClient, aoi: dict, sem: asyncio.Semaphore) -> tuple:
    """Queries S2 and S1 scenes for a single AOI. Returns (aoi_id, epochs_list)."""
    bbox        = aoi['bbox']
    event_start, event_end = aoi['date_range']
    pre_start   = _sub_days(event_start, 30)
    datetime_str = f"{pre_start}T00:00:00Z/{event_end}T23:59:59Z"

    async with sem:
        try:
            # Sentinel-2 L2A query (cloud filtered)
            s2_resp = await client.post(STAC_API_URL, json={
                "collections": ["sentinel-2-l2a"],
                "bbox":        bbox,
                "datetime":    datetime_str,
                "limit":       100,
                "query":       {"eo:cloud_cover": {"lt": 25}},
            }, timeout=60.0)
            s2_items = (
                s2_resp.json().get('features', [])
                if s2_resp.status_code == 200 else []
            )

            # Sentinel-1 GRD query (radar, no cloud filter)
            s1_resp = await client.post(STAC_API_URL, json={
                "collections": ["sentinel-1-grd"],
                "bbox":        bbox,
                "datetime":    datetime_str,
                "limit":       100,
            }, timeout=60.0)
            s1_items = (
                s1_resp.json().get('features', [])
                if s1_resp.status_code == 200 else []
            )

            epochs = select_epochs(s2_items, s1_items, event_start, event_end)
            return aoi['aoi_id'], epochs

        except Exception as e:
            return aoi['aoi_id'], []   # Will be flagged as < 3 epochs


async def _run_all_queries(aois: list, existing: dict) -> dict:
    """Run STAC queries for all AOIs not already catalogued."""
    results  = dict(existing)
    pending  = [a for a in aois if a['aoi_id'] not in results]

    if not pending:
        print("All AOIs already have epoch data. Nothing to do.")
        return results

    print(f"Querying epochs for {len(pending)} AOIs "
          f"(skipping {len(results)} already done)...")

    sem    = asyncio.Semaphore(10)   # Max 10 concurrent STAC requests
    limits = httpx.Limits(max_connections=20, max_keepalive_connections=10)

    async with httpx.AsyncClient(limits=limits) as client:
        tasks     = [_query_one_aoi(client, aoi, sem) for aoi in pending]
        completed = 0

        for coro in asyncio.as_completed(tasks):
            aoi_id, epochs = await coro
            results[aoi_id] = epochs
            completed += 1

            # Checkpoint every 50 AOIs
            if completed % 50 == 0 or completed == len(pending):
                print(f"  {completed}/{len(pending)} queried — saving checkpoint...")
                with open(EPOCHS_PATH, 'w') as f:
                    json.dump(results, f)

    return results


class QueryEpochsPipeline:
    """Builds aoi_epochs.json with 3-5 epochs per AOI. Resumes from checkpoint."""

    def run(self):
        # Load AOI list
        if not os.path.exists(AOI_LIST_PATH):
            raise FileNotFoundError(f"AOI list not found: {AOI_LIST_PATH}")
        with open(AOI_LIST_PATH) as f:
            aois = json.load(f)
        print(f"Loaded {len(aois)} AOIs from {AOI_LIST_PATH}")

        # Load existing checkpoint
        existing = {}
        if os.path.exists(EPOCHS_PATH):
            with open(EPOCHS_PATH) as f:
                existing = json.load(f)
            print(f"Resuming from checkpoint: {len(existing)} AOIs already done.")

        # Windows asyncio fix
        if os.name == 'nt':
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

        results = asyncio.run(_run_all_queries(aois, existing))

        # Final save with pretty-print
        Path(EPOCHS_PATH).parent.mkdir(parents=True, exist_ok=True)
        with open(EPOCHS_PATH, 'w') as f:
            json.dump(results, f, indent=2)

        total     = len(results)
        with_3    = sum(1 for v in results.values() if len(v) >= 3)
        with_5    = sum(1 for v in results.values() if len(v) >= 5)
        no_pre    = sum(1 for v in results.values()
                        if not any(e['epoch_label'] == 'pre_event' for e in v))
        low       = [k for k, v in results.items() if len(v) < 3]

        print(f"\n{'='*60}")
        print(f"[Step 2A Complete] aoi_epochs.json written.")
        print(f"  Total AOIs   : {total}")
        print(f"  >= 3 epochs  : {with_3}  ({with_3/total*100:.1f}%)")
        print(f"  >= 5 epochs  : {with_5}  ({with_5/total*100:.1f}%)")
        print(f"  Missing pre  : {no_pre}")
        if low:
            print(f"  < 3 epochs ({len(low)} AOIs): {low[:10]}")
        print(f"{'='*60}")
