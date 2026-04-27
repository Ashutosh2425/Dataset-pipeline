"""
tdrd/pipelines/step1_select_aois.py
------------------------------------
Modular pipeline for selecting 600 AOIs across 12 disaster events.

Crash-resilience:
  - After each event's STAC phase a checkpoint file is written so that a
    crash during OSMnx road-filtering does not force a full STAC re-scan.
  - Each OSMnx future has a hard 120-second timeout; timed-out tiles are
    silently skipped rather than hanging the pool indefinitely.
  - Each STAC coroutine has a 45-second asyncio timeout.
  - Workers reduced to 4 to limit connection-pool and memory pressure.
"""

import os
import json
import asyncio
import httpx
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError as FutureTimeout
from pathlib import Path

from tdrd.config import EVENTS, EVENT_TARGETS, AOI_LIST_PATH
from tdrd.core.geospatial import generate_tiles, build_xbd_chip_index, tile_overlaps_xbd
from tdrd.core.satellite import check_aoi_coverage
from tdrd.core.networks import check_road_density

XBD_BASE = Path("data/xbd")

CHECKPOINT_DIR = Path("data/step1_checkpoints")


class Step1Pipeline:

    def __init__(self, data_path=AOI_LIST_PATH):
        self.data_path = data_path
        self.selected_aois = self._load_existing()
        CHECKPOINT_DIR.mkdir(parents=True, exist_ok=True)

    def _load_existing(self):
        if os.path.exists(self.data_path):
            with open(self.data_path) as f:
                return json.load(f)
        return []

    def _stac_ckpt_path(self, event_id):
        return CHECKPOINT_DIR / f"stac_{event_id}.json"

    def _save_stac_checkpoint(self, event_id, stac_ok):
        with open(self._stac_ckpt_path(event_id), 'w') as f:
            json.dump(stac_ok, f)

    def _load_stac_checkpoint(self, event_id):
        p = self._stac_ckpt_path(event_id)
        if p.exists():
            with open(p) as f:
                return json.load(f)
        return None

    def _clear_stac_checkpoint(self, event_id):
        p = self._stac_ckpt_path(event_id)
        if p.exists():
            p.unlink()

    def verify_existing(self):
        if not self.selected_aois:
            print("No AOI list found to verify.")
            return False

        print(f"Verifying {len(self.selected_aois)} AOIs...")

        event_xbd = {e['id']: e['xbd_overlap'] for e in EVENTS}
        counts = {}
        bad_scenes = 0
        bad_roads = 0
        xbd_wrong = 0

        for aoi in self.selected_aois:
            eid = aoi['event_id']
            counts[eid] = counts.get(eid, 0) + 1
            if aoi.get('n_sentinel_scenes', 0) < 3:
                bad_scenes += 1
            if aoi.get('n_osm_roads', 0) < 5:
                bad_roads += 1
            if aoi.get('has_xbd_overlap') != event_xbd.get(eid):
                xbd_wrong += 1

        print("\nEvent Counts:")
        for eid, target in EVENT_TARGETS.items():
            current = counts.get(eid, 0)
            status = "[OK]" if current >= target else "[MISSING]"
            print(f"  {eid}: {current}/{target} {status}")

        xbd_true = sum(1 for a in self.selected_aois if a.get('has_xbd_overlap'))
        xbd_false = len(self.selected_aois) - xbd_true
        print(f"\nxBD overlap — True: {xbd_true}, False: {xbd_false}")
        print(f"AOIs with incorrect has_xbd_overlap: {xbd_wrong}")
        print(f"AOIs with < 3 scenes: {bad_scenes}")
        print(f"AOIs with < 5 roads: {bad_roads}")

        return (
            len(self.selected_aois) >= 600
            and bad_scenes == 0
            and bad_roads == 0
            and xbd_wrong == 0
        )

    async def _async_stac_filter(self, tiles, event):
        """Filter tiles by satellite coverage. Each tile gets a 45-second timeout."""
        limits = httpx.Limits(max_connections=50, max_keepalive_connections=25)

        async def safe_check(client, tile):
            try:
                n = await asyncio.wait_for(
                    check_aoi_coverage(client, tile, event['dates']),
                    timeout=30.0
                )
                return (tile, n)
            except (asyncio.TimeoutError, Exception):
                return (tile, 0)

        async with httpx.AsyncClient(limits=limits, timeout=45.0) as client:
            batch_size = 50
            passing = []
            for i in range(0, len(tiles), batch_size):
                batch = tiles[i:i + batch_size]
                results = await asyncio.gather(*[safe_check(client, t) for t in batch])
                passing.extend((t, n) for t, n in results if n >= 3)
                print(f"    STAC: {i + len(batch)}/{len(tiles)} tiles checked, "
                      f"{len(passing)} passing so far")
                await asyncio.sleep(1.5)
            return passing

    def _xbd_filter(self, stac_ok, event):
        """
        Criterion (b): keep only tiles that intersect at least one xBD chip.
        If no xbd_folder is configured, or no chips fall inside the event bbox
        (meaning the folder is a wrong/proxy mapping), the filter is skipped
        and all tiles pass unchanged.
        """
        folder = event.get('xbd_folder')
        if not folder:
            return stac_ok

        chip_bboxes = build_xbd_chip_index(XBD_BASE, folder)
        if not chip_bboxes:
            print(f"    xBD: no chips found in {folder}, skipping filter")
            return stac_ok

        # Auto-detect geographic mismatch: if no chip overlaps the event bbox,
        # this folder is a wrong proxy — skip rather than reject all tiles.
        event_bbox = event['bbox']
        chips_in_region = [c for c in chip_bboxes if tile_overlaps_xbd(event_bbox, [c])]
        if not chips_in_region:
            print(f"    xBD: {folder} chips do not overlap event bbox — skipping filter")
            return stac_ok

        before = len(stac_ok)
        passing = [(t, n) for t, n in stac_ok if tile_overlaps_xbd(t, chips_in_region)]
        print(f"    xBD: {len(passing)}/{before} tiles intersect {folder} chips")
        return passing

    def _road_filter(self, stac_ok, max_pass=None, min_roads=5):
        """
        Filter tiles by road density with early stopping.
        Stops as soon as max_pass tiles pass (2.5× target) — no need to test
        every tile when we already have enough headroom for the trim step.
        Submits in batches of 80 so we don't queue thousands of futures upfront.
        """
        passing = []
        timed_out = 0
        errors = 0
        total = len(stac_ok)
        done = 0
        BATCH = 10

        with ThreadPoolExecutor(max_workers=1) as pool:
            for batch_start in range(0, total, BATCH):
                if max_pass and len(passing) >= max_pass:
                    print(f"    OSMnx: early stop — {len(passing)} passing tiles reached target headroom")
                    break
                batch = stac_ok[batch_start:batch_start + BATCH]
                futs = {pool.submit(check_road_density, t): (t, n) for t, n in batch}
                for fut in as_completed(futs):
                    tile, n_scenes = futs[fut]
                    done += 1
                    try:
                        n_roads = fut.result(timeout=15)
                        if n_roads >= min_roads:
                            passing.append((tile, n_scenes, n_roads))
                    except FutureTimeout:
                        timed_out += 1
                    except Exception:
                        errors += 1
                if done % 20 == 0 or done == total:
                    print(f"    OSMnx: {done}/{total} tiles  "
                          f"pass={len(passing)}  timeout={timed_out}  err={errors}")

        return passing

    def run(self):
        processed_ids = {a['event_id'] for a in self.selected_aois}

        for event in EVENTS:
            eid = event['id']
            if eid in processed_ids:
                print(f"\n{eid}: already done, skipping")
                continue

            print(f"\n{'='*55}")
            print(f"Processing {eid} ({event['name']})...")
            print(f"{'='*55}")
            tiles = generate_tiles(event['bbox'])
            print(f"  Generated {len(tiles)} tiles")

            # Phase 1: STAC — resume from checkpoint if available
            stac_ckpt = self._load_stac_checkpoint(eid)
            if stac_ckpt is not None:
                stac_ok = [tuple(x) for x in stac_ckpt]
                print(f"  Resumed STAC checkpoint: {len(stac_ok)} tiles passing")
            else:
                print(f"  Phase 1: STAC satellite coverage check...")
                stac_ok = asyncio.run(self._async_stac_filter(tiles, event))
                print(f"  STAC done: {len(stac_ok)}/{len(tiles)} tiles pass")
                self._save_stac_checkpoint(eid, stac_ok)

            # Phase 2: xBD spatial overlap (criterion b)
            print(f"  Phase 2: xBD building polygon overlap check...")
            xbd_ok = self._xbd_filter(stac_ok, event)

            # Phase 3: OSMnx road density — stop early once we have 2.5× target
            target = EVENT_TARGETS[eid]
            max_pass = int(target * 2.5)
            min_roads = event.get('min_roads', 5)
            print(f"  Phase 3: OSMnx road density filter ({len(xbd_ok)} tiles, stop at {max_pass}, min_roads={min_roads})...")
            final_ok = self._road_filter(xbd_ok, max_pass=max_pass, min_roads=min_roads)
            print(f"  Phase 3 done: {len(final_ok)} tiles pass all 3 criteria")

            # Add to list and re-assign IDs
            for tile, n_s, n_r in final_ok:
                self.selected_aois.append({
                    'aoi_id': None,
                    'event_id': eid,
                    'event_type': event['type'],
                    'bbox': tile,
                    'date_range': event['dates'],
                    'n_sentinel_scenes': n_s,
                    'n_osm_roads': n_r,
                    'has_xbd_overlap': event['xbd_overlap'],
                })

            for i, aoi in enumerate(self.selected_aois):
                aoi['aoi_id'] = f"{aoi['event_id']}_{i:04d}"

            with open(self.data_path, 'w') as f:
                json.dump(self.selected_aois, f, indent=2)

            self._clear_stac_checkpoint(eid)
            print(f"  Saved. Running total: {len(self.selected_aois)} AOIs")

        print(f"\nStep 1 Complete. Total AOIs: {len(self.selected_aois)}")
