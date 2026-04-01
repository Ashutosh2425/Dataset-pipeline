"""
tdrd/pipelines/select_aois.py
-----------------------------
Modular pipeline for selecting 600 AOIs across 12 disaster events.
"""

import os
import json
import asyncio
import httpx
from concurrent.futures import ProcessPoolExecutor, as_completed

from tdrd.config import EVENTS, EVENT_TARGETS, AOI_LIST_PATH
from tdrd.core.geospatial import generate_tiles
from tdrd.core.satellite import check_aoi_coverage
from tdrd.core.networks import check_road_density

class Step1Pipeline:
    """
    Handles the Step 1 (AOI Selection) logic.
    """
    
    def __init__(self, data_path=AOI_LIST_PATH):
        self.data_path = data_path
        self.selected_aois = self._load_existing()

    def _load_existing(self):
        """Loads existing AOIs if they exist on disk."""
        if os.path.exists(self.data_path):
            with open(self.data_path, 'r') as f:
                return json.load(f)
        return []

    def verify_existing(self):
        """
        Validates that the existing AOI list meets the build guide criteria.
        """
        if not self.selected_aois:
            print("No AOI list found to verify.")
            return False
            
        print(f"Verifying {len(self.selected_aois)} AOIs...")
        
        counts = {}
        bad_scenes = 0
        bad_roads = 0
        
        for aoi in self.selected_aois:
            eid = aoi['event_id']
            counts[eid] = counts.get(eid, 0) + 1
            
            if aoi.get('n_sentinel_scenes', 0) < 3:
                bad_scenes += 1
            if aoi.get('n_osm_roads', 0) < 5:
                bad_roads += 1
                
        # Report results
        print("\nEvent Counts:")
        for eid, target in EVENT_TARGETS.items():
            current = counts.get(eid, 0)
            status = "[OK]" if current >= target else "[MISSING]"
            print(f"  {eid}: {current}/{target} {status}")
            
        print(f"\nAOIs with < 3 scenes: {bad_scenes}")
        print(f"AOIs with < 5 roads: {bad_roads}")
        
        return len(self.selected_aois) >= 600 and bad_scenes == 0 and bad_roads == 0

    async def _async_stac_filter(self, tiles, event):
        """Filters tiles by satellite coverage using STAC."""
        sem = asyncio.Semaphore(10)
        limits = httpx.Limits(max_connections=20)
        
        async with httpx.AsyncClient(limits=limits) as client:
            tasks = [check_aoi_coverage(client, t, event['dates']) for t in tiles]
            results = await asyncio.gather(*tasks)
            return [(t, n) for t, n in zip(tiles, results) if n >= 3]

    def _road_filter(self, stac_ok):
        """Filters tiles by road density using OSMnx."""
        passing = []
        with ProcessPoolExecutor(max_workers=8) as pool:
            futs = {pool.submit(check_road_density, t): (t, n) for t, n in stac_ok}
            for fut in as_completed(futs):
                tile, n_scenes = futs[fut]
                try:
                    n_roads = fut.result()
                    if n_roads >= 5:
                        passing.append((tile, n_scenes, n_roads))
                except Exception:
                    pass
        return passing

    def run(self):
        """
        Runs the full Step 1 pipeline to reach 600 AOIs.
        """
        processed_ids = {a['event_id'] for a in self.selected_aois}
        
        for event in EVENTS:
            if event['id'] in processed_ids:
                continue
                
            print(f"\nProcessing {event['id']} ({event['name']})...")
            tiles = generate_tiles(event['bbox'])
            
            # Phase 1: Satellite coverage (Async STAC)
            stac_ok = asyncio.run(self._async_stac_filter(tiles, event))
            
            # Phase 2: Road network (OSMnx ProcessPool)
            final_ok = self._road_filter(stac_ok)
            
            # Add to list
            for tile, n_s, n_r in final_ok:
                self.selected_aois.append({
                    'aoi_id': None,
                    'event_id': event['id'],
                    'event_type': event['type'],
                    'bbox': tile,
                    'date_range': event['dates'],
                    'n_sentinel_scenes': n_s,
                    'n_osm_roads': n_r,
                    'has_xbd_overlap': False # Placeholder for Step 3 overlap check
                })
                
            # Assign continuous IDs
            for i, aoi in enumerate(self.selected_aois):
                aoi['aoi_id'] = f"{aoi['event_id']}_{i:04d}"
                
            # Incremental save
            with open(self.data_path, 'w') as f:
                json.dump(self.selected_aois, f, indent=2)
                
        print(f"\nStep 1 Complete. Total AOIs: {len(self.selected_aois)}")
