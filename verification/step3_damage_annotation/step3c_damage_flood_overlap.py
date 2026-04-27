"""
Step 3c Verification — Damage vs Flood Spatial Overlap
-------------------------------------------------------
Two output images:

  1. overlap_scatter.png
     Per AOI per epoch: scatter of flood_area (km²) vs high_damage_area (km²).
     For flood events these should be positively correlated.
     Points far off the diagonal = annotation inconsistency.

  2. damage_progression.png
     Per event: line plot of total damaged area (km²) across epochs.
     Should generally increase from during_1 → during_2 for active disasters.
     Flat or random = spectral thresholds may be too noisy.

  3. polygon_size_hist.png
     Histogram of damage polygon areas (m²).
     Very small polygons (< 100 m²) are likely single-pixel noise artifacts.

Run from repo root:
    python verification/step3_damage_annotation/step3c_damage_flood_overlap.py
"""

import json
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from pathlib import Path
from shapely.geometry import shape
from shapely.ops import unary_union
from collections import defaultdict

BASE       = Path(__file__).resolve().parents[2]
ANN_DIR    = BASE / "data" / "annotations"
AOI_LIST   = json.load(open(BASE / "data" / "aoi_list.json"))
EPOCHS_MAP = json.load(open(BASE / "data" / "aoi_epochs.json"))
OUT_DIR    = Path(__file__).parent / "step3c_overlap"
OUT_DIR.mkdir(exist_ok=True)

LABEL_ORDER  = ["during_1", "during_2", "during_3"]
LABEL_COLORS = {"during_1": "#e07b00", "during_2": "#c0392b", "during_3": "#7b241c"}

EVENT_COLORS = {
    "EVT001": "#1f77b4", "EVT002": "#ff7f0e", "EVT003": "#2ca02c",
    "EVT004": "#d62728", "EVT005": "#9467bd", "EVT006": "#8c564b",
    "EVT007": "#e377c2", "EVT008": "#7f7f7f", "EVT009": "#bcbd22",
    "EVT010": "#17becf", "EVT011": "#aec7e8", "EVT012": "#ffbb78",
}

DEG2_TO_KM2 = 111.0 ** 2   # 1 deg² ≈ 12321 km² at equator; use per-feature areas


def load_geoms(path):
    if not path.exists():
        return []
    try:
        feats = json.load(open(path)).get("features", [])
        return [shape(f["geometry"]) for f in feats if f.get("geometry")]
    except Exception:
        return []


def geom_area_km2(geom):
    """Rough area in km² from WGS84 geometry using degree→km conversion."""
    if geom is None or geom.is_empty:
        return 0.0
    bounds = geom.bounds
    lat    = (bounds[1] + bounds[3]) / 2
    km_per_deg_lon = 111.0 * np.cos(np.radians(lat))
    km_per_deg_lat = 111.0
    # Use bounding-box-scaled area (fast approximation)
    return geom.area * km_per_deg_lon * km_per_deg_lat


def epoch_num_from_label(aoi_id, label):
    for i, ep in enumerate(EPOCHS_MAP.get(aoi_id, [])):
        if ep["epoch_label"] == label:
            return i + 1
    return None


def main():
    aoi_map = {a["aoi_id"]: a for a in AOI_LIST}

    # ── Collect per AOI per epoch ────────────────────────────────────────────
    records = []       # (event_id, event_type, aoi_id, epoch_label, flood_km2, hi_dmg_km2, all_dmg_km2)
    poly_areas = []    # all damage polygon areas in m²

    for a in AOI_LIST:
        aoi_id     = a["aoi_id"]
        event_id   = a["event_id"]
        event_type = a["event_type"]
        ann_dir    = ANN_DIR / aoi_id

        for lbl in LABEL_ORDER:
            ep_num = epoch_num_from_label(aoi_id, lbl)
            if ep_num is None:
                continue

            dmg_path = ann_dir / f"damage_polygons_T{ep_num}.geojson"
            fld_path = ann_dir / f"flood_extent_T{ep_num}.geojson"

            dmg_geoms = load_geoms(dmg_path)
            fld_geoms = load_geoms(fld_path)

            # Damage: separate high-damage (class 2+3) from all
            if dmg_path.exists():
                try:
                    feats = json.load(open(dmg_path)).get("features", [])
                    hi_geoms  = [shape(f["geometry"]) for f in feats
                                 if f.get("geometry") and f["properties"].get("damage_class", 0) >= 2]
                    all_geoms = [shape(f["geometry"]) for f in feats if f.get("geometry")]
                    for f in feats:
                        if f.get("geometry"):
                            g = shape(f["geometry"])
                            if not g.is_empty:
                                poly_areas.append(geom_area_km2(g) * 1e6)  # km² → m²
                except Exception:
                    hi_geoms = all_geoms = []
            else:
                hi_geoms = all_geoms = []

            # Union for area calculation
            flood_union = unary_union(fld_geoms) if fld_geoms else None
            hi_union    = unary_union(hi_geoms)  if hi_geoms  else None
            all_union   = unary_union(all_geoms) if all_geoms else None

            flood_km2  = geom_area_km2(flood_union)  if flood_union  else 0.0
            hi_dmg_km2 = geom_area_km2(hi_union)     if hi_union     else 0.0
            all_dmg_km2= geom_area_km2(all_union)    if all_union    else 0.0

            records.append((event_id, event_type, aoi_id, lbl, flood_km2, hi_dmg_km2, all_dmg_km2))

    # ── Plot 1: Overlap scatter ──────────────────────────────────────────────
    fig1, ax1 = plt.subplots(figsize=(9, 7))
    fig1.suptitle(
        "Step 3c — Flood Area vs High-Damage Area per AOI per Epoch\n"
        "Flood events should show positive correlation (points along diagonal)",
        fontsize=10, fontweight="bold"
    )

    for lbl in LABEL_ORDER:
        recs = [(r[0], r[4], r[5]) for r in records if r[3] == lbl and (r[4] > 0 or r[5] > 0)]
        if not recs:
            continue
        eids  = [r[0] for r in recs]
        flood = [r[1] for r in recs]
        hidmg = [r[2] for r in recs]
        colors = [EVENT_COLORS.get(e, "gray") for e in eids]
        ax1.scatter(flood, hidmg, c=colors, alpha=0.5, s=18,
                    label=lbl, marker={"during_1":"o","during_2":"s","during_3":"^"}[lbl])

    mx = max((r[4] for r in records), default=1)
    my = max((r[5] for r in records), default=1)
    lim = max(mx, my) * 1.05
    ax1.plot([0, lim], [0, lim], "k--", lw=1, alpha=0.4, label="1:1 line")
    ax1.set_xlim(0, lim)
    ax1.set_ylim(0, lim)
    ax1.set_xlabel("Flood extent area  (km²)", fontsize=9)
    ax1.set_ylabel("High-damage area (class ≥ 2)  (km²)", fontsize=9)
    ax1.legend(fontsize=8)
    ax1.grid(alpha=0.3, lw=0.5)

    # Add event colour legend
    handles = [plt.Line2D([0],[0], marker="o", color="w",
                          markerfacecolor=EVENT_COLORS.get(eid,"gray"),
                          markersize=7, label=eid)
               for eid in sorted(set(r[0] for r in records))]
    ax1.legend(handles=handles, fontsize=7, loc="upper left",
               ncol=2, title="Event", title_fontsize=7)

    plt.tight_layout(rect=[0, 0, 1, 0.94])
    out1 = OUT_DIR / "overlap_scatter.png"
    fig1.savefig(out1, dpi=130, bbox_inches="tight")
    plt.close(fig1)
    print(f"  {out1.name}")

    # ── Plot 2: Damage progression per event ─────────────────────────────────
    # Sum total damage area across all AOIs per event per epoch
    event_dmg = defaultdict(lambda: defaultdict(float))
    for r in records:
        event_dmg[r[0]][r[3]] += r[6]   # all_dmg_km2

    events = sorted(event_dmg.keys())
    n_cols = 4
    n_rows = (len(events) + n_cols - 1) // n_cols
    x = list(range(len(LABEL_ORDER)))

    fig2, axes2 = plt.subplots(n_rows, n_cols,
                               figsize=(n_cols * 3.6, n_rows * 3.0),
                               squeeze=False)
    fig2.suptitle(
        "Step 3c — Total Damaged Area per Event Across Epochs  (km²)\n"
        "Should increase or peak at during_1/2 for active disasters",
        fontsize=10, fontweight="bold"
    )

    for idx, eid in enumerate(events):
        r, c = divmod(idx, n_cols)
        ax   = axes2[r][c]
        etype = next(a["event_type"] for a in AOI_LIST if a["event_id"] == eid)
        color = EVENT_COLORS.get(eid, "steelblue")
        ys = [event_dmg[eid].get(lbl, 0.0) for lbl in LABEL_ORDER]
        ax.plot(x, ys, color=color, lw=2, marker="o", markersize=6)
        ax.fill_between(x, ys, alpha=0.15, color=color)
        ax.set_xticks(x)
        ax.set_xticklabels(["d1", "d2", "d3"], fontsize=7)
        ax.set_title(f"{eid}  ({etype})", fontsize=8)
        ax.set_ylabel("Total damage area  (km²)", fontsize=7)
        ax.grid(alpha=0.3, lw=0.5)

    for idx in range(len(events), n_rows * n_cols):
        r, c = divmod(idx, n_cols)
        axes2[r][c].set_visible(False)

    plt.tight_layout(rect=[0, 0, 1, 0.94])
    out2 = OUT_DIR / "damage_progression.png"
    fig2.savefig(out2, dpi=130, bbox_inches="tight")
    plt.close(fig2)
    print(f"  {out2.name}")

    # ── Plot 3: Polygon size histogram ───────────────────────────────────────
    fig3, ax3 = plt.subplots(figsize=(9, 4))
    fig3.suptitle(
        "Step 3c — Damage Polygon Size Distribution\n"
        "Very small polygons (< 100 m²) are likely single-pixel noise",
        fontsize=10, fontweight="bold"
    )

    valid = [a for a in poly_areas if a > 0]
    if valid:
        ax3.hist(np.log10(valid), bins=60, color="steelblue", alpha=0.75, edgecolor="none")
        ax3.axvline(np.log10(100),  color="red",    lw=1.5, ls="--", label="100 m²  (noise threshold)")
        ax3.axvline(np.log10(2500), color="orange", lw=1.5, ls="--", label="2500 m²  (one S2 pixel = 10m×10m×25)")
        ax3.set_xlabel("log₁₀(polygon area  m²)", fontsize=9)
        ax3.set_ylabel("Count", fontsize=9)
        noise_frac = sum(1 for a in valid if a < 100) / len(valid)
        ax3.set_title(f"n={len(valid):,} polygons  |  "
                      f"noise fraction (<100 m²): {noise_frac:.1%}", fontsize=9)
        ax3.legend(fontsize=8)
        ax3.grid(alpha=0.3, lw=0.5)

    plt.tight_layout(rect=[0, 0, 1, 0.93])
    out3 = OUT_DIR / "polygon_size_hist.png"
    fig3.savefig(out3, dpi=130, bbox_inches="tight")
    plt.close(fig3)
    print(f"  {out3.name}")

    print(f"\nDone — 3 images in {OUT_DIR}")


if __name__ == "__main__":
    main()
