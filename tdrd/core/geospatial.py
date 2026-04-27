"""
tdrd/core/geospatial.py
-----------------------
Geospatial utilities for AOI generation and geometry manipulation.
"""

import math

def generate_tiles(bbox, tile_size_km=5):
    """
    Tiles a bounding box into square sub-AOIs of a given size.
    
    Args:
        bbox (list): [west, south, east, north] in decimal degrees.
        tile_size_km (int): Size of the square tile in kilometers.
        
    Returns:
        list: A list of tiles, each represented as [west, south, east, north].
    """
    west, south, east, north = bbox
    
    # Calculate degree steps based on latitude center
    lat_center = (south + north) / 2
    deg_per_km_lat = 1.0 / 111.0
    deg_per_km_lon = 1.0 / (111.0 * math.cos(math.radians(lat_center)))
    
    step_lat = tile_size_km * deg_per_km_lat
    step_lon = tile_size_km * deg_per_km_lon
    
    tiles = []
    lat = south
    while lat < north:
        lon = west
        while lon < east:
            tile = [
                round(lon, 6),
                round(lat, 6),
                round(min(lon + step_lon, east), 6),
                round(min(lat + step_lat, north), 6)
            ]
            tiles.append(tile)
            lon += step_lon
        lat += step_lat

    return tiles


import json
import re
from pathlib import Path
from datetime import datetime, timedelta


def build_xbd_chip_index(xbd_base_dir, folder_name):
    """
    Reads all pre-disaster label JSONs for an xBD event folder and returns
    a list of (west, south, east, north) bboxes representing each chip's
    image footprint (~2km × 2km).

    Building polygon coordinates give the chip's centroid location; we expand
    to a fixed 0.02° radius (~2.2km) so that 5km AOI tiles reliably overlap
    chips they contain.
    """
    labels_dir = Path(xbd_base_dir) / folder_name / "labels"
    if not labels_dir.exists():
        return []

    PAD = 0.02  # degrees — ~2.2 km, covers the full 1024-px chip extent

    bboxes = []
    for json_path in labels_dir.glob("*_pre_disaster.json"):
        try:
            data = json.loads(json_path.read_text(encoding="utf-8"))
            features = data.get("features", {})
            items = features.get("lng_lat", []) if isinstance(features, dict) else features

            lons, lats = [], []
            for feat in items:
                wkt = feat.get("wkt", "")
                for lon_s, lat_s in re.findall(r"(-?\d+\.?\d+)\s+(-?\d+\.?\d+)", wkt):
                    lons.append(float(lon_s))
                    lats.append(float(lat_s))

            if lons and lats:
                cx = (min(lons) + max(lons)) / 2
                cy = (min(lats) + max(lats)) / 2
                bboxes.append((cx - PAD, cy - PAD, cx + PAD, cy + PAD))
        except Exception:
            pass

    return bboxes


def tile_overlaps_xbd(tile, chip_bboxes):
    """True if tile [west,south,east,north] intersects at least one chip bbox."""
    w, s, e, n = tile
    return any(w < ce and e > cw and s < cn and n > cs
               for cw, cs, ce, cn in chip_bboxes)

def bbox_to_wkt(bbox):
    """
    Converts [west, south, east, north] to WKT Polygon string.
    """
    west, south, east, north = bbox
    return f"POLYGON(({west} {south}, {east} {south}, {east} {north}, {west} {north}, {west} {south}))"

def subtract_days(date_str, days):
    """
    Subtracts days from a YYYY-MM-DD date string.
    """
    dt = datetime.strptime(date_str, '%Y-%m-%d')
    new_dt = dt - timedelta(days=days)
    return new_dt.strftime('%Y-%m-%d')
