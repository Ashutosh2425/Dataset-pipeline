"""
tdrd/pipelines/analyze_aois.py
------------------------------
Step 3 Prep: Analyzes AOIs for population density (WorldPop) and building footprints (OSM/xBD).
"""

import os
import json
import rasterio
import geopandas as gpd
from shapely.geometry import box
from rasterio.mask import mask
from tdrd.config import AOI_LIST_PATH
from tdrd.core.networks import check_building_density
from tqdm import tqdm

WORLDPOP_DIR = "data/worldpop"

class Step3PrepPipeline:
    """
    Handles the Step 3 Prep logic (Demographics and building counts).
    """
    
    def __init__(self, aoi_path=AOI_LIST_PATH):
        self.aoi_path = aoi_path
        self.aois = self._load_aois()
        self.wp_files = {
            'USA': os.path.join(WORLDPOP_DIR, 'usa_ppp_2020.tif'),
            'IND': os.path.join(WORLDPOP_DIR, 'ind_ppp_2020.tif'),
            'BGD': os.path.join(WORLDPOP_DIR, 'bgd_ppp_2020.tif'),
            'PAK': os.path.join(WORLDPOP_DIR, 'pak_ppp_2020.tif'),
            'TUR': os.path.join(WORLDPOP_DIR, 'tur_ppp_2020.tif'),
            'LBY': os.path.join(WORLDPOP_DIR, 'lby_ppp_2020.tif'),
        }

    def _load_aois(self):
        with open(self.aoi_path, 'r') as f:
            return json.load(f)

    def _get_population(self, bbox, event_id):
        """Extracts population sum from WorldPop TIF for an AOI."""
        # Map event_id to country code
        # EVT001, 003, 005, 008, 011 -> USA
        # EVT002, 004, 007 -> IND (Fani/Amphan are also BGD but IND is main)
        # EVT009 -> PAK
        # EVT010 -> TUR
        # EVT012 -> LBY
        country = None
        if event_id in ['EVT001', 'EVT003', 'EVT005', 'EVT008', 'EVT011']: 
            country = 'USA'
        elif event_id in ['EVT002', 'EVT004', 'EVT007']: 
            country = 'IND'
        elif event_id == 'EVT009': 
            country = 'PAK'
        elif event_id == 'EVT010': 
            country = 'TUR'
        elif event_id == 'EVT012': 
            country = 'LBY'
        
        if not country or country not in self.wp_files or not os.path.exists(self.wp_files[country]):
            return 0
            
        try:
            with rasterio.open(self.wp_files[country]) as src:
                # WGS84 bbox
                geom = [box(*bbox)]
                out_image, out_transform = mask(src, geom, crop=True)
                data = out_image[0]
                # Filter out nodata
                data = data[data > 0]
                return float(data.sum())
        except Exception:
            return 0

    def run(self):
        """Analyzes all AOIs for building count and population."""
        print(f"Starting Step 3 Prep: Analyzing {len(self.aois)} AOIs...")
        
        processed = 0
        for aoi in tqdm(self.aois, desc="Analysis Progress"):
            # Check if already analyzed (optional skip logic)
            if 'population_count' in aoi:
                continue

            bbox = aoi['bbox']

            # 1: Population
            pop = self._get_population(bbox, aoi['event_id'])
            aoi['population_count'] = pop

            # 2: Buildings (OSM)
            bld = check_building_density(bbox)
            aoi['building_count'] = bld

            # 3: Flag xBD overlap if building count > delta or based on event
            # (Just a logic placeholder for true xBD check)
            if aoi['event_id'] in ['EVT001', 'EVT003', 'EVT005', 'EVT006', 'EVT008', 'EVT010']:
                aoi['has_xbd_overlap'] = bld > 10

            # Periodic save every 50 processed
            processed += 1
            if processed % 50 == 0:
                with open(self.aoi_path, 'w') as f:
                    json.dump(self.aois, f, indent=2)

        # Final save
        with open(self.aoi_path, 'w') as f:
            json.dump(self.aois, f, indent=2)
            
        print("Analysis complete. aoi_list.json updated with demographics.")

