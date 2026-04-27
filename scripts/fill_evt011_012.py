"""
Standalone script to fill EVT011 (Hawaii) and EVT012 (Libya) AOIs.
Uses osmnx with cache + built-in rate limiting to avoid Overpass 429.
"""
import sys, json, asyncio, time, socket
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import httpx
import osmnx as ox

socket.setdefaulttimeout(15)
ox.settings.use_cache = True
ox.settings.timeout = 10
ox.settings.log_console = False

from tdrd.config import EVENTS
from tdrd.core.geospatial import generate_tiles
from tdrd.core.satellite import check_aoi_coverage

AOI_PATH = Path("data/aoi_list.json")
CKPT_DIR = Path("data/step1_checkpoints")
CKPT_DIR.mkdir(parents=True, exist_ok=True)

TARGETS = {'EVT011': 35, 'EVT012': 77}
TARGET_EVENTS = {e['id']: e for e in EVENTS if e['id'] in TARGETS}


def count_roads(bbox):
    try:
        G = ox.graph_from_bbox(
            (bbox[0], bbox[1], bbox[2], bbox[3]), network_type='drive')
        return len(G.edges)
    except Exception:
        return 0


async def run_stac(event):
    tiles = generate_tiles(event['bbox'])
    print(f"  STAC: checking {len(tiles)} tiles...", flush=True)
    limits = httpx.Limits(max_connections=30, max_keepalive_connections=15)
    passing = []
    async with httpx.AsyncClient(limits=limits, timeout=45.0) as client:
        for i in range(0, len(tiles), 50):
            batch = tiles[i:i+50]
            tasks = [asyncio.wait_for(
                check_aoi_coverage(client, t, event['dates']), timeout=30.0)
                for t in batch]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for t, res in zip(batch, results):
                if isinstance(res, int) and res >= 3:
                    passing.append((t, res))
            print(f"  STAC: {i+len(batch)}/{len(tiles)} checked, {len(passing)} passing", flush=True)
            await asyncio.sleep(1.5)
    return passing


def load_aois():
    return json.loads(AOI_PATH.read_text()) if AOI_PATH.exists() else []


def save_aois(aois):
    for i, a in enumerate(aois):
        a['aoi_id'] = f"{a['event_id']}_{i:04d}"
    AOI_PATH.write_text(json.dumps(aois, indent=2))


def load_stac_ckpt(eid):
    p = CKPT_DIR / f"stac_{eid}.json"
    return [tuple(x) for x in json.loads(p.read_text())] if p.exists() else None


def save_stac_ckpt(eid, tiles):
    (CKPT_DIR / f"stac_{eid}.json").write_text(json.dumps(tiles))


def run_event(eid):
    event = TARGET_EVENTS[eid]
    target = TARGETS[eid]
    need = target * 2

    aois = load_aois()
    existing_ok = [a for a in aois if a['event_id'] == eid and a.get('n_osm_roads', 0) >= 5]
    if len(existing_ok) >= target:
        print(f"{eid}: already have {len(existing_ok)} qualifying, skipping", flush=True)
        return

    # STAC phase (async)
    stac_tiles = load_stac_ckpt(eid)
    if not stac_tiles:
        print(f"{eid}: running STAC...", flush=True)
        stac_tiles = asyncio.run(run_stac(event))
        save_stac_ckpt(eid, stac_tiles)
        print(f"{eid}: STAC done, {len(stac_tiles)} tiles pass", flush=True)
    else:
        print(f"{eid}: loaded STAC checkpoint ({len(stac_tiles)} tiles)", flush=True)

    # Road phase (synchronous, osmnx with cache)
    print(f"{eid}: checking roads ({len(stac_tiles)} tiles, need {need} with >=5)...", flush=True)
    passing = []
    for i, (tile, n_scenes) in enumerate(stac_tiles):
        if len(passing) >= need:
            print(f"  Early stop: {len(passing)} tiles passing", flush=True)
            break
        n_roads = count_roads(tile)
        if n_roads >= 5:
            passing.append((tile, n_scenes, n_roads))
        if (i + 1) % 5 == 0:
            print(f"  [{i+1}/{len(stac_tiles)}] pass={len(passing)} roads={n_roads}", flush=True)
        time.sleep(0.5)

    print(f"{eid}: {len(passing)} tiles with roads>=5", flush=True)

    # Save
    aois = load_aois()
    aois = [a for a in aois if a['event_id'] != eid]
    for tile_, n_s, n_r in passing:
        aois.append({
            'aoi_id': None,
            'event_id': eid,
            'event_type': event['type'],
            'bbox': list(tile_),
            'date_range': event['dates'],
            'n_sentinel_scenes': n_s,
            'n_osm_roads': n_r,
            'has_xbd_overlap': event['xbd_overlap'],
        })
    save_aois(aois)
    print(f"{eid}: saved to aoi_list.json", flush=True)


if __name__ == '__main__':
    for eid in ['EVT011', 'EVT012']:
        run_event(eid)
    print("\nAll done. Run: python scripts/trim_aoi_list.py", flush=True)
