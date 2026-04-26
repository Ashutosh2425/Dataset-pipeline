"""
tdrd/config.py
--------------
Central configuration for the Temporal Disaster Response Dataset (TDRD).
12 disaster events per build guide §1.1.
"""

# 600 AOIs across 12 events — per-event targets (build guide §1.1)
EVENT_TARGETS = {
    'EVT001': 68,  # Hurricane Harvey        (flood,       USA)
    'EVT002': 52,  # Kerala Floods           (flood,       India)
    'EVT003': 41,  # Camp Fire               (fire,        USA)
    'EVT004': 44,  # Cyclone Fani            (wind+flood,  India)
    'EVT005': 55,  # Midwest Floods          (flood,       USA)
    'EVT006': 28,  # Beirut Explosion        (structural,  Lebanon)
    'EVT007': 48,  # Cyclone Amphan          (wind+flood,  India/Bangladesh)
    'EVT008': 22,  # Natchez Tornadoes       (wind,        USA)
    'EVT009': 72,  # Pakistan Floods         (flood,       Pakistan)
    'EVT010': 58,  # Turkey Earthquake       (structural,  Turkey)
    'EVT011': 35,  # Hawaii Wildfires        (fire,        USA)
    'EVT012': 77,  # Libya Derna Floods      (flash flood, Libya)
}

EVENTS = [
    {
        'id': 'EVT001',
        'name': 'hurricane_harvey',
        'bbox': [-96.1, 29.2, -94.5, 30.4],   # expanded — Harvey impacted broader Houston metro
        'dates': ('2017-08-01', '2017-09-30'),
        'type': 'flood',
        'xbd_folder': 'hurricane-harvey',
        'xbd_overlap': True,
    },
    {
        'id': 'EVT002',
        'name': 'kerala_floods',
        'bbox': [75.8, 9.5, 77.5, 11.5],
        'dates': ('2018-07-01', '2018-09-30'),
        'type': 'flood',
        'xbd_folder': None,
        'xbd_overlap': False,
    },
    {
        'id': 'EVT003',
        'name': 'santa_rosa_wildfire',
        'bbox': [-122.95, 38.30, -122.55, 38.70],
        'dates': ('2017-09-01', '2017-12-31'),
        'type': 'fire',
        'xbd_folder': 'santa-rosa-wildfire',
        'xbd_overlap': True,
    },
    {
        'id': 'EVT004',
        'name': 'cyclone_fani',
        'bbox': [85.0, 19.8, 86.5, 21.0],
        'dates': ('2019-04-01', '2019-06-30'),
        'type': 'wind+flood',
        'xbd_folder': None,
        'xbd_overlap': False,
    },
    {
        'id': 'EVT005',
        'name': 'midwest_flooding_2019',
        'bbox': [-96.5, 34.5, -92.0, 36.6],
        'dates': ('2019-03-01', '2019-05-31'),
        'type': 'flood',
        'xbd_folder': 'midwest-flooding',
        'xbd_overlap': True,
    },
    {
        'id': 'EVT006',
        'name': 'beirut_explosion',
        'bbox': [35.2, 33.7, 35.9, 34.2],     # expanded — covers Greater Beirut + Mount Lebanon suburbs
        'dates': ('2020-07-15', '2020-10-31'),
        'type': 'structural',
        'xbd_folder': None,
        'xbd_overlap': False,
    },
    {
        'id': 'EVT007',
        'name': 'cyclone_amphan',
        'bbox': [88.0, 21.5, 89.8, 23.2],
        'dates': ('2020-04-01', '2020-07-31'),
        'type': 'wind+flood',
        'xbd_folder': None,
        'xbd_overlap': False,
    },
    {
        'id': 'EVT008',
        'name': 'hurricane_michael',
        'bbox': [-86.0, 29.8, -85.3, 30.6],
        'dates': ('2018-09-01', '2018-12-31'),
        'type': 'wind+flood',
        'xbd_folder': 'hurricane-michael',
        'xbd_overlap': True,
    },
    {
        'id': 'EVT009',
        'name': 'nepal_flooding',
        'bbox': [83.00, 26.30, 83.80, 27.20],
        'dates': ('2017-06-01', '2017-10-31'),
        'type': 'flood',
        'xbd_folder': 'nepal-flooding',
        'xbd_overlap': True,
    },
    {
        'id': 'EVT010',
        'name': 'mexico_earthquake',
        'bbox': [-99.45, 19.10, -98.85, 19.65],
        'dates': ('2017-07-01', '2017-11-30'),
        'type': 'structural',
        'xbd_folder': 'mexico-earthquake',
        'xbd_overlap': True,
    },
    {
        'id': 'EVT011',
        'name': 'hawaii_wildfires',
        'bbox': [-157.1, 20.6, -155.9, 21.2],   # expanded to capture more road-dense tiles
        'dates': ('2023-07-01', '2023-10-31'),
        'type': 'fire',
        'xbd_folder': None,
        'xbd_overlap': False,
    },
    {
        'id': 'EVT012',
        'name': 'hurricane_florence',
        'bbox': [-79.50, 33.30, -77.50, 35.20],
        'dates': ('2018-08-01', '2018-12-31'),
        'type': 'wind+flood',
        'xbd_folder': 'hurricane-florence',
        'xbd_overlap': True,
    },
]

AOI_LIST_PATH = "data/aoi_list.json"
XBD_FOOTPRINTS_PATH = "data/xbd_event_footprints.geojson"
STAC_API_URL = "https://earth-search.aws.element84.com/v1/search"
