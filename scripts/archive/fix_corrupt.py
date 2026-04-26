import json
from pathlib import Path
import geopandas as gpd
import sys
sys.path.insert(0, '.')
from tdrd.pipelines.step3b_flood_extraction import ndwi_flood, build_sen1floods_index, extract_flood_from_sen1floods

aoi_id = 'EVT009_0400'
with open('data/aoi_list.json') as f:
    aois = {a['aoi_id']: a for a in json.load(f)}
aoi = aois[aoi_id]
with open(f'data/stacks/{aoi_id}_meta.json') as f:
    meta = json.load(f)

index = build_sen1floods_index()
out_path = Path(f'data/annotations/{aoi_id}/flood_extent_T2.geojson')
out_path.unlink(missing_ok=True)

flood_gdf = extract_flood_from_sen1floods(aoi_id, aoi['bbox'], index)
if flood_gdf is None or len(flood_gdf) == 0:
    flood_gdf = ndwi_flood(aoi_id, meta, f'data/stacks/{aoi_id}_stack.tif', 1)
if flood_gdf is not None and len(flood_gdf) > 0:
    flood_gdf['epoch'] = 2
    flood_gdf.to_file(str(out_path), driver='GeoJSON')
    print('Regenerated OK')
else:
    empty = gpd.GeoDataFrame(columns=['geometry','source','epoch'], geometry='geometry').set_crs('EPSG:4326')
    empty.to_file(str(out_path), driver='GeoJSON')
    print('Written empty')
