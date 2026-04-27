"""
tdrd/core/networks.py
---------------------
Geospatial network utilities and OSM logic.
"""

import socket
import osmnx as ox

socket.setdefaulttimeout(12)

def check_road_density(bbox, network_type='drive'):
    """
    Checks the number of road edges within a bounding box.
    """
    try:
        ox.settings.use_cache = True
        ox.settings.timeout = 10
        G = ox.graph_from_bbox(
            north=bbox[3], south=bbox[1], east=bbox[2], west=bbox[0],
            network_type=network_type
        )
        return len(G.edges)
    except Exception:
        return 0

def check_building_density(bbox):
    """
    Checks the number of building polygons within a bounding box using OSM.
    """
    try:
        ox.settings.timeout = 30
        tags = {"building": True}
        gdf = ox.geometries_from_bbox(
            north=bbox[3], south=bbox[1], east=bbox[2], west=bbox[0],
            tags=tags
        )
        return len(gdf)
    except Exception:
        return 0
