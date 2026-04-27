"""
step7_query_generation.py
-------------------------
Generate 6 natural-language query types per AOI, each paired with a
deterministic GT tool-chain and verified answer.

Query types:
  Q1  Counting    — damage polygon count at epoch T
  Q2  Area        — flood/burn area in km² at epoch T
  Q3  Proximity   — closest shelter to most-damaged zone
  Q4  Change      — flood boundary change between T and T+2
  Q5  Routing     — safest route from centroid to shelter
  Q6  Conflict    — routes safe at T but blocked by T+1

LLM strategy (1 call per AOI, not per query):
  All 6 gt_answer_texts are generated in a single batched prompt per AOI.
  Paraphrases use deterministic templates (no LLM cost).
  Total wall time: ~5-7 hours on CPU with qwen3:8b.

Output: data/queries_raw.jsonl  (~3 600 records, one per AOI × query type)
"""

import json
import math
import random
import warnings
import concurrent.futures
from pathlib import Path
from collections import defaultdict

from shapely.geometry import shape
from shapely.ops import unary_union

warnings.filterwarnings("ignore")

BASE      = Path(".")
ANN_DIR   = BASE / "data" / "annotations"
STACKS    = BASE / "data" / "stacks"
AOI_LIST  = json.load(open(BASE / "data" / "aoi_list.json"))
CONFLICTS = json.load(open(BASE / "data" / "conflict_events.json"))

QUERY_TYPES = ["Q1_counting", "Q2_area", "Q3_proximity",
               "Q4_change",   "Q5_routing", "Q6_conflict"]

AGENTS = {
    "Q1_counting":  ["VRA", "ORC"],
    "Q2_area":      ["VRA", "ORC"],
    "Q3_proximity": ["VRA", "GA", "ORC"],
    "Q4_change":    ["VRA", "ORC"],
    "Q5_routing":   ["VRA", "GA", "PA", "ORC"],
    "Q6_conflict":  ["VRA", "GA", "PA", "ORC"],
}

# 4 deterministic paraphrase templates per query type.
# Use the same format-map kwargs as the base query.
PARAPHRASE_TEMPLATES = {
    "Q1_counting": [
        "What is the count of {cond} {obj} in {loc} at {epoch}?",
        "In {loc} at {epoch}, how many {obj} show {cond} damage?",
        "Give the total number of {cond} {obj} recorded in {loc} as of {date}.",
        "How many {obj} in {loc} were classified as {cond} during {epoch}?",
    ],
    "Q2_area": [
        "How many km² are covered by {hazard} in {loc} at {epoch}?",
        "Calculate the {hazard} extent in {loc} at {epoch}.",
        "What is the {hazard}-affected surface area in {loc} as of {date}?",
        "Report the total {hazard} footprint in {loc} at {epoch} in square kilometres.",
    ],
    "Q3_proximity": [
        "What is the nearest shelter to the highest-damage area in {loc}?",
        "Identify the closest evacuation shelter to the most damaged part of {loc}.",
        "Find the shortest-distance shelter from the most damaged zone in {loc}.",
        "Which facility is nearest to the peak-damage zone in {loc}?",
    ],
    "Q4_change": [
        "What was the change in {hazard} extent from {ep1} to {ep2} in {loc}?",
        "Compare the {hazard} coverage between {ep1} and {ep2} for {loc}.",
        "By how much did the {hazard}-affected area change from {ep1} to {ep2} in {loc}?",
        "Describe the {hazard} boundary evolution between {ep1} and {ep2} in {loc}.",
    ],
    "Q5_routing": [
        "What is the safest route from {centroid} to the nearest shelter during {epoch}?",
        "Identify the lowest-risk evacuation path from {centroid} to a shelter at {epoch}.",
        "Which route from {centroid} to a shelter has the highest road safety score at {epoch}?",
        "Find the optimal evacuation corridor from {centroid} to a shelter at {epoch}.",
    ],
    "Q6_conflict": [
        "List the evacuation routes that were passable at {ep1} but became impassable by {ep2} in {loc}.",
        "Identify routes in {loc} that degraded from safe to blocked between {ep1} and {ep2}.",
        "Which routes in {loc} were viable at {ep1} but no longer safe by {ep2}?",
        "What evacuation paths in {loc} lost safety between {ep1} and {ep2}?",
    ],
}

random.seed(42)

OLLAMA_URL   = "http://localhost:11434"
OLLAMA_MODEL = "qwen3:8b"


# ── LLM interface ──────────────────────────────────────────────────────────────

def _check_ollama():
    import requests
    try:
        r = requests.get(f"{OLLAMA_URL}/api/tags", timeout=5)
        models = r.json().get("models", [])
    except Exception as e:
        raise RuntimeError(
            f"Ollama not reachable at {OLLAMA_URL}. "
            "Run: ollama serve"
        ) from e
    if not models:
        raise RuntimeError(
            f"Ollama running but no models loaded. Run: ollama pull {OLLAMA_MODEL}"
        )
    names = [m["name"] for m in models]
    match = next((n for n in names if OLLAMA_MODEL.split(":")[0] in n), names[0])
    print(f"  Ollama model: {match}")
    return match


def _llm(prompt, model, temperature=0.3, timeout=300):
    import requests
    payload = {
        "model": model, "prompt": prompt,
        "stream": False, "temperature": temperature,
    }
    if "qwen3" in model:
        payload["think"] = False  # disable CoT — 3-5x faster on CPU
    resp = requests.post(
        f"{OLLAMA_URL}/api/generate",
        json=payload,
        timeout=timeout,
    )
    return resp.json()["response"].strip()


def llm_answer_texts_batch(qt_answer_map, model):
    """
    One LLM call per AOI: convert all applicable GT answer dicts to
    natural-language sentences.

    qt_answer_map: {query_type: gt_answer_dict, ...}
    Returns:       {query_type: answer_text_str, ...}
    """
    lines = "\n".join(
        f"{qt}: {json.dumps(ans)}"
        for qt, ans in qt_answer_map.items()
    )
    prompt = (
        "Convert each structured GT answer below to a concise 1-2 sentence "
        "natural-language response for a remote-sensing analysis query. "
        "Be precise with numbers. No markdown. "
        "Return ONLY a JSON object mapping query_type to answer string.\n\n"
        + lines
    )
    raw   = _llm(prompt, model)
    start = raw.find("{")
    end   = raw.rfind("}") + 1
    try:
        result = json.loads(raw[start:end])
        return result if isinstance(result, dict) else {}
    except Exception:
        return {}


# ── GT data loading ────────────────────────────────────────────────────────────

def _polygon_area_km2(features):
    geoms = [shape(f["geometry"]) for f in features if f.get("geometry")]
    if not geoms:
        return 0.0
    union  = unary_union(geoms)
    bounds = union.bounds
    lat    = (bounds[1] + bounds[3]) / 2
    km_lon = 111.0 * math.cos(math.radians(lat))
    return union.area * km_lon * 111.0


def load_gt_data(aoi_id):
    ann = ANN_DIR / aoi_id
    gt  = {"damage": {}, "flood": {}, "routes": {}, "conflicts": []}
    for ep in [2, 3, 4]:
        dp = ann / f"damage_polygons_T{ep}.geojson"
        fe = ann / f"flood_extent_T{ep}.geojson"
        if dp.exists():
            gt["damage"][f"T{ep}"] = json.load(open(dp)).get("features", [])
        if fe.exists():
            gt["flood"][f"T{ep}"] = json.load(open(fe)).get("features", [])
    rjp = ann / "evacuation_routes.json"
    if rjp.exists():
        gt["routes"] = json.load(open(rjp))
    gt["conflicts"] = [c for c in CONFLICTS if c["aoi_id"] == aoi_id]
    return gt


# ── GT answer builders ─────────────────────────────────────────────────────────

def _gt_q1(aoi_id, gt, epoch="T2"):
    feats = gt["damage"].get(epoch, [])
    return {
        "epoch":              epoch,
        "total_polygons":     len(feats),
        "major_or_destroyed": sum(1 for f in feats
                                  if f.get("properties", {}).get("damage_class", 0) >= 2),
        "destroyed_only":     sum(1 for f in feats
                                  if f.get("properties", {}).get("damage_class", 0) >= 3),
    }


def _gt_q2(aoi_id, gt, epoch="T2"):
    return {"epoch": epoch,
            "flood_area_km2": round(_polygon_area_km2(gt["flood"].get(epoch, [])), 4)}


def _gt_q3(aoi_id, gt):
    all_routes = [r for rl in gt["routes"].values() for r in rl]
    if not all_routes:
        return None
    best = min(all_routes, key=lambda r: r.get("length_m", 1e9))
    epoch = next((ep for ep, rl in gt["routes"].items() if best in rl), "T2")
    return {
        "shelter_name":   best.get("shelter_name", "unknown"),
        "route_length_m": best.get("length_m", 0),
        "eta_min":        best.get("eta_min", 0),
        "epoch":          epoch,
    }


def _gt_q4(aoi_id, gt, epoch_T="T2", epoch_T1="T4"):
    area_T  = _polygon_area_km2(gt["flood"].get(epoch_T,  []))
    area_T1 = _polygon_area_km2(gt["flood"].get(epoch_T1, []))
    delta   = area_T1 - area_T
    pct     = (delta / area_T * 100) if area_T > 0 else 0.0
    return {
        f"flood_area_{epoch_T}_km2":  round(area_T,  4),
        f"flood_area_{epoch_T1}_km2": round(area_T1, 4),
        "delta_km2":   round(delta, 4),
        "change_pct":  round(pct, 2),
        "direction":   "increase" if delta > 0 else "decrease" if delta < 0 else "unchanged",
    }


def _gt_q5(aoi_id, gt, epoch="T2"):
    rlist = gt["routes"].get(epoch, [])
    if not rlist:
        return None
    best = max(rlist, key=lambda r: (r.get("rss", 0), -r.get("length_m", 1e9)))
    return {
        "epoch":        epoch,
        "shelter_name": best.get("shelter_name", "unknown"),
        "length_m":     best.get("length_m", 0),
        "eta_min":      best.get("eta_min", 0),
        "rss":          best.get("rss", 1.0),
        "geometry":     best.get("geometry", {}),
    }


def _gt_q6(aoi_id, gt):
    c = gt["conflicts"]
    if not c:
        return None
    return {
        "n_conflicts":        len(c),
        "epoch_pairs":        list({(x["epoch_T"], x["epoch_T1"]) for x in c}),
        "invalidated_routes": [x["route_id"] for x in c],
        "score_deltas":       [x["score_delta"] for x in c],
    }


# ── Tool-chain metadata ────────────────────────────────────────────────────────

def build_gt_tool_chain(query_type, aoi_id, epoch):
    base = [{"agent": "VRA", "action": "load_stack",
             "params": {"aoi_id": aoi_id, "epoch": epoch}}]
    if query_type == "Q1_counting":
        return base + [{"agent": "ORC", "action": "count_damage_polygons",
                        "params": {"file": f"damage_polygons_{epoch}.geojson",
                                   "filter": {"damage_class__gte": 2}}}]
    if query_type == "Q2_area":
        return base + [{"agent": "ORC", "action": "compute_flood_area_km2",
                        "params": {"file": f"flood_extent_{epoch}.geojson"}}]
    if query_type == "Q3_proximity":
        return base + [
            {"agent": "GA",  "action": "load_road_graph",   "params": {"aoi_id": aoi_id}},
            {"agent": "ORC", "action": "find_nearest_shelter",
             "params": {"source": "evacuation_routes.json", "criterion": "min_length_m"}},
        ]
    if query_type == "Q4_change":
        ep2 = "T4" if epoch == "T2" else "T3"
        return base + [
            {"agent": "VRA", "action": "load_stack",
             "params": {"aoi_id": aoi_id, "epoch": ep2}},
            {"agent": "ORC", "action": "compute_area_delta",
             "params": {"file_T": f"flood_extent_{epoch}.geojson",
                        "file_T1": f"flood_extent_{ep2}.geojson"}},
        ]
    if query_type == "Q5_routing":
        return base + [
            {"agent": "GA", "action": "load_road_graph",    "params": {"aoi_id": aoi_id}},
            {"agent": "PA", "action": "compute_safest_route",
             "params": {"source": "population_centroids.json",
                        "target": "shelter_pois.json", "epoch": epoch}},
            {"agent": "ORC", "action": "verify_rss",
             "params": {"file": "evacuation_routes.json", "epoch": epoch}},
        ]
    if query_type == "Q6_conflict":
        return base + [
            {"agent": "GA", "action": "load_road_graph",    "params": {"aoi_id": aoi_id}},
            {"agent": "PA", "action": "compare_epoch_routes",
             "params": {"epoch_T": epoch, "epoch_T1": "T4"}},
            {"agent": "ORC", "action": "lookup_conflict_events",
             "params": {"file": "data/conflict_events.json", "aoi_id": aoi_id}},
        ]
    return base


# ── Per-AOI query generation ───────────────────────────────────────────────────

def _fmt_date(raw):
    if len(raw) == 8:
        return f"{raw[:4]}-{raw[4:6]}-{raw[6:]}"
    return raw


def generate_queries_for_aoi(aoi, llm_model=None):
    aoi_id     = aoi["aoi_id"]
    event_type = aoi["event_type"]
    meta_path  = STACKS / f"{aoi_id}_meta.json"
    if not meta_path.exists():
        return []

    meta  = json.load(open(meta_path))
    dates = meta.get("dates", [])
    gt    = load_gt_data(aoi_id)

    date_T2 = _fmt_date(dates[1]) if len(dates) > 1 else "post-event"
    loc     = aoi_id
    hazard  = ("flooding" if "flood" in event_type else
               "fire"     if event_type == "fire"   else
               "structural damage")
    obj     = "buildings" if "structural" in event_type else "structures"

    kwargs = dict(
        obj=obj, cond="major or destroyed", loc=loc,
        epoch="T2", date=date_T2, hazard=hazard,
        ep1="T2", ep2="T4",
        centroid=f"the population centre in {loc}",
    )

    BASE_TEMPLATES = {
        "Q1_counting":  "How many {obj} are {cond} in {loc} at {epoch}?",
        "Q2_area":      "What is the total area affected by {hazard} in {loc} at {epoch}?",
        "Q3_proximity": "Which shelter is closest to the most damaged zone in {loc}?",
        "Q4_change":    "How did the {hazard} boundary change between {ep1} and {ep2} in {loc}?",
        "Q5_routing":   "Plan the safest evacuation route from {centroid} to a shelter at {epoch}.",
        "Q6_conflict":  "Which evacuation routes are safe at {ep1} but blocked by {ep2} in {loc}?",
    }

    # Build GT answers for all applicable query types
    gt_answers = {}
    for qtype in QUERY_TYPES:
        if qtype == "Q1_counting":
            gt_answers[qtype] = _gt_q1(aoi_id, gt)
        elif qtype == "Q2_area":
            if gt["flood"].get("T2"):
                gt_answers[qtype] = _gt_q2(aoi_id, gt)
        elif qtype == "Q3_proximity":
            if any(gt["routes"].values()):
                ans = _gt_q3(aoi_id, gt)
                if ans:
                    gt_answers[qtype] = ans
        elif qtype == "Q4_change":
            if gt["flood"].get("T2") and gt["flood"].get("T4"):
                gt_answers[qtype] = _gt_q4(aoi_id, gt)
        elif qtype == "Q5_routing":
            if gt["routes"].get("T2"):
                ans = _gt_q5(aoi_id, gt)
                if ans:
                    gt_answers[qtype] = ans
        elif qtype == "Q6_conflict":
            if gt["conflicts"]:
                ans = _gt_q6(aoi_id, gt)
                if ans:
                    gt_answers[qtype] = ans

    if not gt_answers:
        return []

    # Single batched LLM call for all answer texts
    answer_texts = {}
    if llm_model:
        try:
            answer_texts = llm_answer_texts_batch(gt_answers, llm_model)
        except Exception:
            pass  # fall through with empty answer_texts

    # Assemble records
    queries = []
    for qtype, gt_answer in gt_answers.items():
        base_q = BASE_TEMPLATES[qtype].format_map(defaultdict(str, **kwargs))
        paras  = [
            t.format_map(defaultdict(str, **kwargs))
            for t in PARAPHRASE_TEMPLATES[qtype]
        ]
        queries.append({
            "aoi_id":          aoi_id,
            "event_id":        aoi["event_id"],
            "event_type":      event_type,
            "query_type":      qtype,
            "query":           base_q,
            "paraphrases":     paras,
            "gt_tool_chain":   build_gt_tool_chain(qtype, aoi_id, "T2"),
            "gt_answer":       gt_answer,
            "gt_answer_text":  answer_texts.get(qtype, ""),
            "agents_required": AGENTS[qtype],
        })

    return queries


# ── Pipeline ───────────────────────────────────────────────────────────────────

class Step7QueryPipeline:
    def __init__(self, workers=2, base_dir="."):
        self.workers  = workers
        self.base_dir = str(Path(base_dir).resolve())

    def run(self):
        out_path  = Path(self.base_dir) / "data" / "queries_raw.jsonl"
        llm_model = _check_ollama()

        # Resume: skip AOIs that already have gt_answer_text populated
        done_aois = set()
        if out_path.exists():
            with open(out_path, encoding="utf-8") as f:
                for line in f:
                    try:
                        rec = json.loads(line)
                        if rec.get("gt_answer_text"):   # only count complete records
                            done_aois.add(rec["aoi_id"])
                    except Exception:
                        pass
        remaining = [a for a in AOI_LIST if a["aoi_id"] not in done_aois]
        if done_aois:
            print(f"  Resuming: {len(done_aois)} AOIs already done, "
                  f"{len(remaining)} remaining")
        print(f"Step 7: generating queries with {llm_model}  (1 LLM call/AOI)")

        # Wipe incomplete records from the file, keep only done AOIs
        if done_aois and out_path.exists():
            with open(out_path, encoding="utf-8") as f:
                kept = [l for l in f
                        if json.loads(l).get("aoi_id") in done_aois]
            with open(out_path, "w", encoding="utf-8") as f:
                f.writelines(kept)

        all_queries = []
        completed   = 0

        # Open output file for appending — write each AOI immediately
        out_f = open(out_path, "a", encoding="utf-8")
        try:
            with concurrent.futures.ThreadPoolExecutor(max_workers=self.workers) as ex:
                futures = {
                    ex.submit(generate_queries_for_aoi, a, llm_model): a["aoi_id"]
                    for a in remaining
                }
                for fut in concurrent.futures.as_completed(futures):
                    aoi_id = futures[fut]
                    try:
                        qs = fut.result()
                        for q in qs:
                            out_f.write(json.dumps(q) + "\n")
                        out_f.flush()
                        all_queries.extend(qs)
                    except Exception as e:
                        print(f"  ERROR {aoi_id}: {e}")
                    completed += 1
                    if completed % 50 == 0 or completed == len(remaining):
                        print(f"  {completed}/{len(remaining)} AOIs this run | "
                              f"{len(all_queries)} new queries")
        finally:
            out_f.close()

        # Summary over full file
        all_in_file = []
        with open(out_path, encoding="utf-8") as f:
            for line in f:
                try:
                    all_in_file.append(json.loads(line))
                except Exception:
                    pass

        by_type = defaultdict(int)
        for q in all_in_file:
            by_type[q["query_type"]] += 1

        print(f"\nTotal queries : {len(all_in_file)}")
        print(f"Target        : ~3600  (600 AOIs × 6 types)")
        print(f"\nBy type:")
        for qt in QUERY_TYPES:
            print(f"  {qt:<18} {by_type[qt]}")
        print(f"\nSaved -> {out_path}")
        return all_in_file
