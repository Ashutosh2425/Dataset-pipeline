"""Quick smoke test: score 3 AOIs and verify output."""
import json, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from tdrd.pipelines.step4_road_damage_scoring import score_aoi

BASE = str(Path(__file__).resolve().parents[1])
aois = json.load(open(Path(BASE) / 'data' / 'aoi_list.json'))[:3]

for aoi in aois:
    meta = json.load(open(Path(BASE) / 'data' / 'stacks' / f'{aoi["aoi_id"]}_meta.json'))
    epochs = list(range(2, meta['n_epochs'] + 1))
    result = score_aoi(aoi['aoi_id'], aoi['bbox'], epochs, BASE, str(Path(BASE) / 'data' / 'osm_cache'))
    print('Result:', result)
