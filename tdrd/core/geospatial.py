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


from datetime import datetime, timedelta

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
