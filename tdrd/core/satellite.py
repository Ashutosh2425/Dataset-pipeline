"""
tdrd/core/satellite.py
----------------------
Satellite data acquisition and search.
"""

import httpx
import re
from tdrd.config import STAC_API_URL

async def search_stac_items(client, bbox, date_range, collections, cloud_filter=None):
    """
    Searches the STAC API and returns the full item features.
    """
    start, end = date_range
    dt = f"{start}T00:00:00Z/{end}T23:59:59Z"
    
    payload = {
        "collections": collections,
        "bbox": bbox,
        "datetime": dt,
        "limit": 100
    }
    
    if cloud_filter is not None:
        payload["query"] = {"eo:cloud_cover": {"lt": cloud_filter}}
        
    try:
        response = await client.post(STAC_API_URL, json=payload, timeout=60.0)
        if response.status_code == 200:
            return response.json().get('features', [])
    except Exception as e:
        print(f"STAC search error: {e}")
        
    return []

async def get_best_scenes(client, aoi, cloud_threshold=25):
    """
    Finds the best S1 and S2 scenes for a specific AOI.
    """
    bbox = aoi['bbox']
    dates = aoi['date_range']
    
    s2_items = await search_stac_items(client, bbox, dates, ["sentinel-2-l2a"], cloud_filter=cloud_threshold)
    s2_items = sorted(s2_items, key=lambda x: x['properties'].get('eo:cloud_cover', 100))
    
    s1_items = await search_stac_items(client, bbox, dates, ["sentinel-1-grd"])
    s1_items = sorted(s1_items, key=lambda x: x['properties'].get('datetime', ''), reverse=True)
    
    return {
        's2_match': s2_items[0] if s2_items else None,
        's1_match': s1_items[0] if s1_items else None,
        's2_count': len(s2_items),
        's1_count': len(s1_items)
    }

async def check_aoi_coverage(client, bbox, date_range, cloud_threshold=25):
    """
    Checks if an AOI has enough Sentinel-1 and Sentinel-2 scenes.
    """
    s2_items = await search_stac_items(client, bbox, date_range, ["sentinel-2-l2a"], cloud_filter=cloud_threshold)
    s1_items = await search_stac_items(client, bbox, date_range, ["sentinel-1-grd"])
    return len(s2_items) + len(s1_items)

def extract_date_from_filename(filename):
    """
    Extracts YYYYMMDD from Sentinel filename.
    Example: S2A_MSIL2A_20170828T163901_N0205_R069_T15RUT_20170828T164505.SAFE
    """
    match = re.search(r'(\d{8})T', filename)
    if match:
        return match.group(1)
    return "00000000"
