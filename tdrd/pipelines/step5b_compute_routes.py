"""
step5b_compute_routes.py
------------------------
Build damage-weighted road graphs and compute top-3 A* evacuation routes
from population centroids to nearest intact shelters.

Graph source: OSM cache GraphML (has topology) + scores from Step 4 GPKG.

Output: data/annotations/{aoi_id}/evacuation_routes.json
"""

import json
import sqlite3
import warnings
import concurrent.futures
from pathlib import Path

import networkx as nx
import osmnx as ox

from tdrd.pipelines.step5a_extract_pois import extract_and_cache

warnings.filterwarnings('ignore')

IMPASSABLE_THRESHOLD = 0.60
DAMAGE_WEIGHT_FACTOR = 3.0   # damage score multiplier on travel cost
WALKING_SPEED_KPH    = 5.0


def _cache_key(bbox):
    return '_'.join(f'{v:.3f}' for v in bbox)


def _load_scores_from_gpkg(gpkg_path, epoch):
    """
    Read osmid → score mapping from GPKG via sqlite3 (avoids fiona issues).
    Returns dict {osmid: float}.
    """
    score_col = f'score_T{epoch}'
    scores = {}
    try:
        con = sqlite3.connect(gpkg_path)
        cols = [r[1] for r in con.execute('PRAGMA table_info("road_damage_scores")').fetchall()]
        if score_col not in cols:
            con.close()
            return scores
        rows = con.execute(f'SELECT osmid, "{score_col}" FROM road_damage_scores').fetchall()
        con.close()
    except Exception:
        return scores

    for osmid_raw, score in rows:
        if score is None:
            continue
        raw_str = str(osmid_raw)
        try:
            osmids = json.loads(raw_str) if raw_str.startswith('[') else [int(osmid_raw)]
        except Exception:
            osmids = [osmid_raw]
        for oid in osmids:
            scores[oid] = max(scores.get(oid, 0.0), float(score))

    return scores


def build_weighted_graph(aoi_id, bbox, epoch, base_dir, cache_dir):
    """
    Load GraphML from OSM cache, annotate edges with Step 4 damage scores,
    remove impassable edges (score >= 0.60), and set travel-cost weights.
    Returns the annotated graph, or None if no OSM data for this AOI.
    """
    cache_path = Path(cache_dir) / f'{_cache_key(bbox)}.graphml'
    if not cache_path.exists():
        return None

    G = ox.load_graphml(cache_path)
    if len(G.nodes) == 0:
        return None

    gpkg_path = Path(base_dir) / 'data' / 'annotations' / aoi_id / 'road_damage_scores.gpkg'
    osmid_scores = _load_scores_from_gpkg(gpkg_path, epoch) if gpkg_path.exists() else {}

    edges_to_remove = []
    for u, v, k, data in G.edges(keys=True, data=True):
        raw = data.get('osmid', None)
        if raw is None:
            score = 0.0
        else:
            raw_str = str(raw)
            try:
                oids = json.loads(raw_str) if raw_str.startswith('[') else [int(raw)]
            except Exception:
                oids = [raw]
            score = max((osmid_scores.get(oid, 0.0) for oid in oids), default=0.0)

        if score >= IMPASSABLE_THRESHOLD:
            edges_to_remove.append((u, v, k))
        else:
            length = float(data.get('length', 100))
            G[u][v][k]['weight']       = (1.0 + score * DAMAGE_WEIGHT_FACTOR) * length
            G[u][v][k]['damage_score'] = score

    G.remove_edges_from(edges_to_remove)
    if len(G.nodes) > 0:
        lcc = max(nx.weakly_connected_components(G), key=len)
        G = G.subgraph(lcc).copy()
    return G


def _heuristic(G, u, v):
    """Euclidean distance heuristic for A* (metres, approximate)."""
    ud = G.nodes[u]; vd = G.nodes[v]
    dlat = float(ud['y']) - float(vd['y'])
    dlon = float(ud['x']) - float(vd['x'])
    return ((dlat ** 2 + dlon ** 2) ** 0.5) * 111_000


def find_routes(G, origin_lat, origin_lon, shelters, top_k=3):
    """
    A* from one centroid to each shelter; return top_k sorted by
    (RSS descending, length ascending) — safest first, then shortest.
    """
    if len(G.nodes) == 0:
        return []

    try:
        origin_node = ox.nearest_nodes(G, origin_lon, origin_lat)
    except Exception:
        return []

    routes = []
    for shelter in shelters:
        try:
            dest_node = ox.nearest_nodes(G, shelter['lon'], shelter['lat'])
        except Exception:
            continue
        if dest_node == origin_node:
            continue
        try:
            path = nx.astar_path(
                G, origin_node, dest_node,
                weight='weight',
                heuristic=lambda u, v, _G=G: _heuristic(_G, u, v),
            )
        except (nx.NetworkXNoPath, nx.NodeNotFound):
            continue

        seg_lengths = [
            float(next(iter(G[path[i]][path[i + 1]].values())).get('length', 100))
            for i in range(len(path) - 1)
        ]
        seg_scores = [
            float(next(iter(G[path[i]][path[i + 1]].values())).get('damage_score', 0.0))
            for i in range(len(path) - 1)
        ]
        total_m = sum(seg_lengths)
        rss = (sum(1 for s in seg_scores if s < 0.60) /
               max(len(seg_scores), 1))

        coords = [(float(G.nodes[n]['x']), float(G.nodes[n]['y'])) for n in path]

        routes.append({
            'shelter_name':  shelter.get('name', 'unknown'),
            'shelter_amenity': shelter.get('amenity', 'unknown'),
            'path_nodes':    path,
            'length_m':      round(total_m, 1),
            'eta_min':       round(total_m / 1000 / WALKING_SPEED_KPH * 60, 1),
            'rss':           round(rss, 4),
            'geometry':      {'type': 'LineString', 'coordinates': coords},
        })

    routes.sort(key=lambda r: (-r['rss'], r['length_m']))
    return routes[:top_k]


def process_aoi(aoi_id, bbox, epochs, base_dir, cache_dir):
    """Run full Step 5 for one AOI: POI extraction + route computation."""
    out_path = Path(base_dir) / 'data' / 'annotations' / aoi_id / 'evacuation_routes.json'
    if out_path.exists():
        return str(out_path)

    ann_dir = Path(base_dir) / 'data' / 'annotations' / aoi_id
    extract_and_cache(aoi_id, bbox, ann_dir)

    centroids_path = ann_dir / 'population_centroids.json'
    shelters_path  = ann_dir / 'shelter_pois.json'

    centroids = json.loads(centroids_path.read_text()) if centroids_path.exists() else []
    shelters  = json.loads(shelters_path.read_text())  if shelters_path.exists()  else []

    if not centroids or not shelters:
        out_path.write_text(json.dumps({}))
        return str(out_path)

    all_routes = {}
    for ep in epochs:
        G = build_weighted_graph(aoi_id, bbox, ep, base_dir, cache_dir)
        if G is None or len(G.nodes) == 0:
            continue  # omit epoch key entirely — no road graph exists

        epoch_routes = []
        for centroid in centroids:
            routes = find_routes(G, centroid['lat'], centroid['lon'], shelters)
            epoch_routes.extend(routes)
        all_routes[f'T{ep}'] = epoch_routes

    out_path.write_text(json.dumps(all_routes, indent=2))
    print(f'  OK {aoi_id}: epochs {epochs}, '
          f'{sum(len(v) for v in all_routes.values())} routes total')
    return str(out_path)


class Step5EvacuationPipeline:
    def __init__(self, workers=2, base_dir='.'):
        self.workers   = workers
        self.base_dir  = str(Path(base_dir).resolve())
        self.cache_dir = str(Path(base_dir) / 'data' / 'osm_cache')
        self.aoi_list  = json.load(open(Path(base_dir) / 'data' / 'aoi_list.json'))

    def _epochs(self, aoi_id):
        meta_path = Path(self.base_dir) / 'data' / 'stacks' / f'{aoi_id}_meta.json'
        if not meta_path.exists():
            return []
        return list(range(2, json.load(open(meta_path))['n_epochs'] + 1))

    def run(self, force=False, event_ids=None):
        """
        force: re-run all AOIs even if routes exist.
        event_ids: if given (set of str), only process AOIs from those events.
        """
        if force or event_ids:
            pending = []
            for a in self.aoi_list:
                if event_ids and a['event_id'] not in event_ids:
                    continue
                rpath = Path(self.base_dir) / 'data' / 'annotations' / a['aoi_id'] / 'evacuation_routes.json'
                if force and rpath.exists():
                    rpath.unlink()
                pending.append(a)
        else:
            pending = [
                a for a in self.aoi_list
                if not (Path(self.base_dir) / 'data' / 'annotations' / a['aoi_id'] / 'evacuation_routes.json').exists()
            ]
        done = len(self.aoi_list) - len(pending)
        print(f'Step 5: {len(pending)} AOIs to process ({done} already done)')

        completed = 0
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.workers) as ex:
            futures = {
                ex.submit(
                    process_aoi,
                    a['aoi_id'], a['bbox'], self._epochs(a['aoi_id']),
                    self.base_dir, self.cache_dir
                ): a['aoi_id']
                for a in pending
            }
            for fut in concurrent.futures.as_completed(futures):
                aoi_id = futures[fut]
                try:
                    fut.result()
                except Exception as e:
                    print(f'  ERROR {aoi_id}: {e}')
                completed += 1
                if completed % 50 == 0 or completed == len(pending):
                    print(f'Progress: {completed + done}/{len(self.aoi_list)}')

        print('Step 5 complete.')