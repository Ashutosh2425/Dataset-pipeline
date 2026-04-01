"""
tdrd/config.py
--------------
Central configuration for the Temporal Disaster Response Dataset (TDRD).
Contains definitions for the 12 disaster events and their target AOI counts.
"""

# 600 AOIs across 12 events — per-event targets
EVENT_TARGETS = {
    'EVT001': 68,  # Hurricane Harvey
    'EVT002': 52,  # Kerala Floods
    'EVT003': 41,  # Camp Fire
    'EVT004': 44,  # Cyclone Fani
    'EVT005': 55,  # Midwest Floods
    'EVT006': 28,  # Beirut Explosion
    'EVT007': 48,  # Cyclone Amphan
    'EVT008': 22,  # Natchez Tornadoes
    'EVT009': 72,  # Pakistan Floods
    'EVT010': 58,  # Turkey Earthquake
    'EVT011': 35,  # Hawaii Wildfires
    'EVT012': 77,  # Libya Derna Floods
}

# Bounding boxes and date ranges for satellite data acquisition
EVENTS = [
    {
        'id': 'EVT001',
        'name': 'hurricane_harvey',
        'bbox': [-95.9, 29.5, -95.0, 30.2],
        'dates': ('2017-08-01', '2017-09-30'),
        'type': 'flood'
    },
    {
        'id': 'EVT002',
        'name': 'kerala_floods',
        'bbox': [75.5, 8.5, 77.5, 11.0],
        'dates': ('2018-07-01', '2018-09-30'),
        'type': 'flood'
    },
    {
        'id': 'EVT003',
        'name': 'camp_fire',
        'bbox': [-121.7, 39.6, -121.2, 40.1],
        'dates': ('2018-10-01', '2018-12-31'),
        'type': 'fire'
    },
    {
        'id': 'EVT004',
        'name': 'cyclone_fani',
        'bbox': [85.0, 19.5, 86.5, 21.0],
        'dates': ('2019-04-01', '2019-06-30'),
        'type': 'wind+flood'
    },
    {
        'id': 'EVT005',
        'name': 'midwest_floods',
        'bbox': [-96.5, 40.5, -95.0, 42.0],
        'dates': ('2019-03-01', '2019-05-31'),
        'type': 'flood'
    },
    {
        'id': 'EVT006',
        'name': 'beirut_explosion',
        'bbox': [35.45, 33.85, 35.55, 33.93],
        'dates': ('2020-07-15', '2020-10-31'),
        'type': 'structural'
    },
    {
        'id': 'EVT007',
        'name': 'cyclone_amphan',
        'bbox': [87.5, 21.5, 89.5, 23.5],
        'dates': ('2020-04-01', '2020-07-31'),
        'type': 'wind+flood'
    },
    {
        'id': 'EVT008',
        'name': 'natchez_tornadoes',
        'bbox': [-91.5, 31.3, -90.8, 32.0],
        'dates': ('2021-04-01', '2021-06-30'),
        'type': 'wind'
    },
    {
        'id': 'EVT009',
        'name': 'pakistan_floods',
        'bbox': [66.0, 24.0, 71.0, 29.0],
        'dates': ('2022-06-01', '2022-11-30'),
        'type': 'flood'
    },
    {
        'id': 'EVT010',
        'name': 'turkey_earthquake',
        'bbox': [36.5, 36.5, 38.5, 38.0],
        'dates': ('2023-01-01', '2023-04-30'),
        'type': 'structural'
    },
    {
        'id': 'EVT011',
        'name': 'hawaii_wildfires',
        'bbox': [-156.9, 20.6, -155.9, 21.2],
        'dates': ('2023-07-01', '2023-10-31'),
        'type': 'fire'
    },
    {
        'id': 'EVT012',
        'name': 'libya_derna_floods',
        'bbox': [22.3, 32.5, 23.3, 33.1],
        'dates': ('2023-09-01', '2023-11-30'),
        'type': 'flood'
    },
]

AOI_LIST_PATH = "data/aoi_list.json"
XBD_FOOTPRINTS_PATH = "data/xbd_event_footprints.geojson"
STAC_API_URL = "https://earth-search.aws.element84.com/v1/search"
