import sys, json, time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from tdrd.pipelines.step4_road_damage_scoring import score_aoi

BASE  = str(Path(__file__).resolve().parents[1])
CACHE = str(Path(BASE) / 'data' / 'osm_cache')
aois  = json.load(open(Path(BASE) / 'data' / 'aoi_list.json'))
test  = [a for a in aois if not (Path(BASE) / 'data' / 'annotations' / a['aoi_id'] / 'road_damage_scores.gpkg').exists()][:2]
for aoi in test:
    meta   = json.load(open(Path(BASE) / 'data' / 'stacks' / f'{aoi["aoi_id"]}_meta.json'))
    epochs = list(range(2, meta['n_epochs'] + 1))
    t0     = time.time()
    score_aoi(aoi['aoi_id'], aoi['bbox'], epochs, BASE, CACHE)
    print(f'Time: {time.time()-t0:.1f}s')
