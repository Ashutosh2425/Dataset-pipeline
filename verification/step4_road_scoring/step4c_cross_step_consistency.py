"""
Step 4c Verification — Cross-Step Consistency (Steps 2 → 3 → 4)
-----------------------------------------------------------------
Checks whether the whole pipeline forms a coherent chain:

  NDWI water fraction (Step 2)
       ↓  should drive
  Flood extent area (Step 3)
       ↓  should drive
  % impassable roads (Step 4)

Three scatter plots, one per link in the chain.
Each point = one AOI × one post-event epoch.
Flood events should show positive correlation throughout.
Flat / random scatter = a step is disconnected from the others.

Also plots a temporal line: mean % impassable roads per event per epoch,
to check whether road damage peaks at during_1/2 and potentially recovers.

Output: verification/step4_road_scoring/step4c_consistency/

Run from repo root:
    python verification/step4_road_scoring/step4c_cross_step_consistency.py
"""

import json
import sqlite3
import numpy as np
import rasterio
from rasterio.enums import Resampling
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from pathlib import Path
from shapely.geometry import shape
from shapely.ops import unary_union
from collections import defaultdict

BASE       = Path(__file__).resolve().parents[2]
STACKS_DIR = BASE / "data" / "stacks"
ANN_DIR    = BASE / "data" / "annotations"
AOI_LIST   = json.load(open(BASE / "data" / "aoi_list.json"))
EPOCHS_MAP = json.load(open(BASE / "data" / "aoi_epochs.json"))
OUT_DIR    = Path(__file__).parent / "step4c_consistency"
OUT_DIR.mkdir(exist_ok=True)

LABEL_ORDER = ["during_1", "during_2", "during_3"]
DOWNSAMPLE  = 4
NDWI_THRESH = 0.2

EVENT_COLORS = {
    "EVT001": "#1f77b4", "EVT002": "#ff7f0e", "EVT003": "#2ca02c",
    "EVT004": "#d62728", "EVT005": "#9467bd", "EVT006": "#8c564b",
    "EVT007": "#e377c2", "EVT008": "#7f7f7f", "EVT009": "#bcbd22",
    "EVT010": "#17becf", "EVT011": "#aec7e8", "EVT012": "#ffbb78",
}

GPKG_HEADER_SIZES = {0: 0, 1: 32, 2: 48, 3: 48, 4: 64}


def parse_geom(blob):
    if not blob:
        return None
    try:
        env_type = (blob[3] >> 1) & 0x07
        offset = 8 + GPKG_HEADER_SIZES.get(env_type, 0)
        from shapely.wkb import loads as wkb_loads
        return wkb_loads(blob[offset:])
    except Exception:
        return None


def epoch_num_from_label(aoi_id, label):
    for i, ep in enumerate(EPOCHS_MAP.get(aoi_id, [])):
        if ep["epoch_label"] == label:
            return i + 1
    return None


def ndwi_fraction(tif_path, epoch_idx, bpe):
    if epoch_idx >= len(bpe) or bpe[epoch_idx] < 4:
        return None
    offset = sum(bpe[:epoch_idx])
    with rasterio.open(tif_path) as src:
        H, W = src.height, src.width
        oh, ow = max(1, H // DOWNSAMPLE), max(1, W // DOWNSAMPLE)
        green = src.read(offset + 2, out_shape=(1, oh, ow),
                         resampling=Resampling.average)[0].astype(np.float32)
        nir   = src.read(offset + 4, out_shape=(1, oh, ow),
                         resampling=Resampling.average)[0].astype(np.float32)
    denom = green + nir
    ndwi  = np.where(denom > 0, (green - nir) / denom, 0.0)
    return float((ndwi > NDWI_THRESH).mean())


def flood_area_km2(ann_dir, ep_num):
    path = ann_dir / f"flood_extent_T{ep_num}.geojson"
    if not path.exists():
        return 0.0
    try:
        feats = json.load(open(path)).get("features", [])
        geoms = [shape(f["geometry"]) for f in feats if f.get("geometry")]
        if not geoms:
            return 0.0
        union = unary_union(geoms)
        bounds = union.bounds
        lat = (bounds[1] + bounds[3]) / 2
        km_lon = 111.0 * np.cos(np.radians(lat))
        return union.area * km_lon * 111.0
    except Exception:
        return 0.0


def road_pct_impassable(gpkg_path, score_col, status_col):
    if not gpkg_path.exists():
        return None
    conn = sqlite3.connect(gpkg_path)
    cur  = conn.cursor()
    cols = [d[0] for d in cur.execute("SELECT * FROM road_damage_scores LIMIT 0").description]
    if status_col not in cols:
        conn.close()
        return None
    si  = cols.index(status_col)
    rows = cur.execute("SELECT * FROM road_damage_scores").fetchall()
    conn.close()
    if not rows:
        return None
    total = len(rows)
    imp   = sum(1 for r in rows if r[si] == "impassable")
    return imp / total


def main():
    print("Collecting per-AOI per-epoch metrics (this reads all TIFs at 1/4 res)...")

    records = []
    for a in AOI_LIST:
        aoi_id     = a["aoi_id"]
        event_id   = a["event_id"]
        event_type = a["event_type"]
        tif_path   = STACKS_DIR / f"{aoi_id}_stack.tif"
        meta_path  = STACKS_DIR / f"{aoi_id}_meta.json"
        gpkg_path  = ANN_DIR / aoi_id / "road_damage_scores.gpkg"
        ann_dir    = ANN_DIR / aoi_id

        if not (tif_path.exists() and meta_path.exists()):
            continue
        meta = json.load(open(meta_path))
        bpe  = meta["bands_per_epoch"]

        for lbl in LABEL_ORDER:
            ep_num = epoch_num_from_label(aoi_id, lbl)
            if ep_num is None:
                continue
            ep_idx = ep_num - 1

            ndwi_frac = ndwi_fraction(tif_path, ep_idx, bpe)
            flood_km2 = flood_area_km2(ann_dir, ep_num)
            pct_imp   = road_pct_impassable(
                gpkg_path,
                f"score_T{ep_num}",
                f"status_T{ep_num}"
            )

            if ndwi_frac is None:
                continue

            records.append({
                "event_id":   event_id,
                "event_type": event_type,
                "aoi_id":     aoi_id,
                "label":      lbl,
                "ndwi_frac":  ndwi_frac,
                "flood_km2":  flood_km2,
                "pct_imp":    pct_imp,
            })

        if len(records) % 100 == 0:
            print(f"  {len(records)} records so far...")

    print(f"Total records: {len(records)}")

    def scatter(ax, xs, ys, eids, title, xlabel, ylabel):
        colors = [EVENT_COLORS.get(e, "gray") for e in eids]
        ax.scatter(xs, ys, c=colors, alpha=0.4, s=12, linewidths=0)
        ax.set_xlabel(xlabel, fontsize=8)
        ax.set_ylabel(ylabel, fontsize=8)
        ax.set_title(title, fontsize=8, fontweight="bold")
        ax.grid(alpha=0.3, lw=0.5)
        # Pearson r
        xv = np.array(xs, dtype=float)
        yv = np.array(ys, dtype=float)
        mask = np.isfinite(xv) & np.isfinite(yv)
        if mask.sum() > 5:
            r = np.corrcoef(xv[mask], yv[mask])[0, 1]
            ax.text(0.97, 0.04, f"r = {r:.2f}", transform=ax.transAxes,
                    ha="right", va="bottom", fontsize=8,
                    bbox=dict(boxstyle="round,pad=0.2", fc="white", alpha=0.7))

    # ── Plot 1: Three scatter plots (chain) ──────────────────────────────────
    fig1, axes = plt.subplots(1, 3, figsize=(15, 5))
    fig1.suptitle(
        "Step 4c — Cross-Step Consistency: NDWI → Flood Extent → Road Damage\n"
        "Each point = one AOI × one epoch.  Colour = event.  r = Pearson correlation.",
        fontsize=10, fontweight="bold"
    )

    # Link 1: NDWI fraction → flood area
    xs1 = [r["ndwi_frac"]         for r in records]
    ys1 = [r["flood_km2"]         for r in records]
    eids= [r["event_id"]          for r in records]
    scatter(axes[0], xs1, ys1, eids,
            "Step 2 → Step 3\nNDWI water fraction vs flood extent area",
            "NDWI water fraction", "Flood extent area  (km²)")

    # Link 2: flood area → % impassable
    mask2 = [r["pct_imp"] is not None for r in records]
    xs2   = [r["flood_km2"] for r in records if r["pct_imp"] is not None]
    ys2   = [r["pct_imp"]   for r in records if r["pct_imp"] is not None]
    eids2 = [r["event_id"]  for r in records if r["pct_imp"] is not None]
    scatter(axes[1], xs2, ys2, eids2,
            "Step 3 → Step 4\nFlood extent area vs % impassable roads",
            "Flood extent area  (km²)", "% impassable roads")

    # Link 3: NDWI fraction → % impassable (end-to-end)
    xs3   = [r["ndwi_frac"] for r in records if r["pct_imp"] is not None]
    ys3   = [r["pct_imp"]   for r in records if r["pct_imp"] is not None]
    eids3 = [r["event_id"]  for r in records if r["pct_imp"] is not None]
    scatter(axes[2], xs3, ys3, eids3,
            "End-to-end: Step 2 → Step 4\nNDWI water fraction vs % impassable roads",
            "NDWI water fraction", "% impassable roads")

    # Shared event colour legend
    handles = [plt.Line2D([0],[0], marker="o", color="w",
                          markerfacecolor=EVENT_COLORS.get(eid,"gray"),
                          markersize=7, label=eid)
               for eid in sorted(set(r["event_id"] for r in records))]
    fig1.legend(handles=handles, fontsize=7, loc="lower center",
                ncol=6, bbox_to_anchor=(0.5, -0.02))

    plt.tight_layout(rect=[0, 0.06, 1, 0.93])
    out1 = OUT_DIR / "chain_scatter.png"
    fig1.savefig(out1, dpi=130, bbox_inches="tight")
    plt.close(fig1)
    print(f"  {out1.name}")

    # ── Plot 2: Temporal road damage progression per event ───────────────────
    event_imp = defaultdict(lambda: defaultdict(list))
    for r in records:
        if r["pct_imp"] is not None:
            event_imp[r["event_id"]][r["label"]].append(r["pct_imp"])

    events = sorted(event_imp.keys())
    n_cols = 4
    n_rows = (len(events) + n_cols - 1) // n_cols
    x = list(range(len(LABEL_ORDER)))

    fig2, axes2 = plt.subplots(n_rows, n_cols,
                               figsize=(n_cols * 3.6, n_rows * 3.0),
                               squeeze=False)
    fig2.suptitle(
        "Step 4c — % Impassable Roads per Event Across Epochs\n"
        "Should peak at during_1 or during_2 for flood/structural events",
        fontsize=10, fontweight="bold"
    )

    for idx, eid in enumerate(events):
        r, c = divmod(idx, n_cols)
        ax   = axes2[r][c]
        etype = next(a["event_type"] for a in AOI_LIST if a["event_id"] == eid)
        color = EVENT_COLORS.get(eid, "steelblue")

        means = []
        for lbl in LABEL_ORDER:
            vals = event_imp[eid][lbl]
            means.append(np.mean(vals) * 100 if vals else np.nan)

        ax.plot(x, means, color=color, lw=2.5, marker="o", markersize=7)
        ax.fill_between(x, means, alpha=0.15, color=color)
        ax.set_xticks(x)
        ax.set_xticklabels(["d1", "d2", "d3"], fontsize=7)
        ax.set_ylim(0, 100)
        ax.set_ylabel("% impassable", fontsize=7)
        ax.set_title(f"{eid}  ({etype})", fontsize=8)
        ax.grid(alpha=0.3, lw=0.5)

    for idx in range(len(events), n_rows * n_cols):
        r, c = divmod(idx, n_cols)
        axes2[r][c].set_visible(False)

    plt.tight_layout(rect=[0, 0, 1, 0.94])
    out2 = OUT_DIR / "road_damage_temporal.png"
    fig2.savefig(out2, dpi=130, bbox_inches="tight")
    plt.close(fig2)
    print(f"  {out2.name}")

    print(f"\nDone — 2 images in {OUT_DIR}")


if __name__ == "__main__":
    main()
