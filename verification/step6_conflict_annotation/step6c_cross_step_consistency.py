"""
Step 6c Verification — Cross-Step Consistency (Steps 3/4/5 -> 6)
-----------------------------------------------------------------
Checks whether conflict episodes are consistent with upstream signals:

  Chain verified:
    Flood extent area     (Step 3)
         | more flood -> more conflict events
    % Impassable roads    (Step 4)
         | more blocked roads -> more conflicts
    Route length          (Step 5)
         | longer routes -> more conflict exposure
    Conflict count        (Step 6)

  Four scatter plots (each point = one AOI):
    1. % impassable roads (Step 4)  vs  conflict count per AOI
    2. Flood extent km2  (Step 3)   vs  conflict count per AOI
    3. Mean route length (Step 5)   vs  conflict count per AOI
    4. Conflict count               vs  event type (box plots)

  One temporal plot:
    Conflicts per epoch pair (T2->T3 vs T3->T4) per event type

Output: verification/step6_conflict_annotation/step6c_consistency/

Run from repo root:
    python verification/step6_conflict_annotation/step6c_cross_step_consistency.py
"""

import json
import sqlite3
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from pathlib import Path
from collections import defaultdict
from shapely.geometry import shape
from shapely.ops import unary_union

BASE      = Path(__file__).resolve().parents[2]
ANN_DIR   = BASE / "data" / "annotations"
AOI_LIST  = json.load(open(BASE / "data" / "aoi_list.json"))
CONFLICTS = json.load(open(BASE / "data" / "conflict_events.json"))
OUT_DIR   = Path(__file__).parent / "step6c_consistency"
OUT_DIR.mkdir(exist_ok=True)

aoi_map = {a["aoi_id"]: a for a in AOI_LIST}

EVENT_COLORS = {
    "EVT001": "#1f77b4", "EVT002": "#ff7f0e", "EVT003": "#2ca02c",
    "EVT004": "#d62728", "EVT005": "#9467bd", "EVT006": "#8c564b",
    "EVT007": "#e377c2", "EVT008": "#7f7f7f", "EVT009": "#bcbd22",
    "EVT010": "#17becf", "EVT011": "#aec7e8", "EVT012": "#ffbb78",
}


def flood_area_km2(aoi_id, ep_num):
    path = ANN_DIR / aoi_id / f"flood_extent_T{ep_num}.geojson"
    if not path.exists():
        return 0.0
    try:
        feats = json.load(open(path)).get("features", [])
        geoms = [shape(f["geometry"]) for f in feats if f.get("geometry")]
        if not geoms:
            return 0.0
        union  = unary_union(geoms)
        bounds = union.bounds
        lat    = (bounds[1] + bounds[3]) / 2
        km_lon = 111.0 * np.cos(np.radians(lat))
        return union.area * km_lon * 111.0
    except Exception:
        return 0.0


def pct_impassable(gpkg_path, ep_num):
    if not gpkg_path.exists():
        return None
    col = f"status_T{ep_num}"
    try:
        con  = sqlite3.connect(gpkg_path)
        cols = [d[0] for d in con.execute(
            "SELECT * FROM road_damage_scores LIMIT 0").description]
        if col not in cols:
            con.close(); return None
        si   = cols.index(col)
        rows = con.execute("SELECT * FROM road_damage_scores").fetchall()
        con.close()
        if not rows:
            return None
        return sum(1 for r in rows if r[si] == "impassable") / len(rows)
    except Exception:
        return None


def main():
    print("Collecting per-AOI metrics...")

    # Count conflicts per AOI and epoch pair
    aoi_conflicts = defaultdict(int)
    aoi_T2T3      = defaultdict(int)
    aoi_T3T4      = defaultdict(int)
    for c in CONFLICTS:
        aoi_conflicts[c["aoi_id"]] += 1
        if c["epoch_T"] == 2:
            aoi_T2T3[c["aoi_id"]] += 1
        else:
            aoi_T3T4[c["aoi_id"]] += 1

    records = []
    for a in AOI_LIST:
        aoi_id    = a["aoi_id"]
        event_id  = a["event_id"]
        event_type = a["event_type"]
        gpkg_path = ANN_DIR / aoi_id / "road_damage_scores.gpkg"
        rjp       = ANN_DIR / aoi_id / "evacuation_routes.json"

        # Step 3: max flood area across epochs
        fl_km2 = max(flood_area_km2(aoi_id, e) for e in [2, 3, 4])

        # Step 4: mean % impassable across epochs
        imps = [pct_impassable(gpkg_path, e) for e in [2, 3, 4]]
        pct_imp = np.mean([v for v in imps if v is not None]) if any(v is not None for v in imps) else None

        # Step 5: mean route length
        mean_len = None
        if rjp.exists():
            r = json.load(open(rjp))
            lens = [x.get("length_m", 0) for ep, rlist in r.items() for x in rlist]
            mean_len = np.mean(lens) if lens else None

        records.append({
            "aoi_id":     aoi_id,
            "event_id":   event_id,
            "event_type": event_type,
            "fl_km2":     fl_km2,
            "pct_imp":    pct_imp,
            "mean_len":   mean_len,
            "n_conflicts": aoi_conflicts.get(aoi_id, 0),
        })

    print(f"  {len(records)} AOI records built.")

    def scatter_ax(ax, xs, ys, eids, title, xlabel, r_label=True):
        cs = [EVENT_COLORS.get(e, "gray") for e in eids]
        ax.scatter(xs, ys, c=cs, alpha=0.4, s=14, linewidths=0)
        ax.set_xlabel(xlabel, fontsize=8)
        ax.set_ylabel("Conflicts per AOI", fontsize=8)
        ax.set_title(title, fontsize=8, fontweight="bold")
        ax.grid(alpha=0.3, lw=0.5)
        if r_label and len(xs) > 5:
            xv = np.array(xs, dtype=float)
            yv = np.array(ys, dtype=float)
            mask = np.isfinite(xv) & np.isfinite(yv)
            if mask.sum() > 5:
                r = np.corrcoef(xv[mask], yv[mask])[0, 1]
                ax.text(0.97, 0.05, f"r = {r:.2f}",
                        transform=ax.transAxes, ha="right", va="bottom",
                        fontsize=8,
                        bbox=dict(boxstyle="round,pad=0.2", fc="white", alpha=0.7))

    # ── Plot 1: Four scatter plots ────────────────────────────────────────────
    fig1, axes = plt.subplots(2, 2, figsize=(12, 9))
    fig1.suptitle(
        "Step 6c — Cross-Step Consistency: Upstream Signals -> Conflict Count\n"
        "Each point = one AOI.  Colour = event.  r = Pearson correlation.",
        fontsize=10, fontweight="bold"
    )

    # 1a: % impassable vs conflicts
    sub = [(r["pct_imp"], r["n_conflicts"], r["event_id"])
           for r in records if r["pct_imp"] is not None]
    scatter_ax(axes[0][0],
               [x[0] for x in sub], [x[1] for x in sub], [x[2] for x in sub],
               "Step 4 -> Step 6\n% Impassable Roads vs Conflict Count",
               "% impassable roads (mean across epochs)")

    # 1b: flood area vs conflicts
    sub = [(r["fl_km2"], r["n_conflicts"], r["event_id"]) for r in records]
    scatter_ax(axes[0][1],
               [x[0] for x in sub], [x[1] for x in sub], [x[2] for x in sub],
               "Step 3 -> Step 6\nFlood Extent vs Conflict Count",
               "Max flood extent area (km2)")

    # 1c: route length vs conflicts
    sub = [(r["mean_len"], r["n_conflicts"], r["event_id"])
           for r in records if r["mean_len"] is not None]
    scatter_ax(axes[1][0],
               [x[0] for x in sub], [x[1] for x in sub], [x[2] for x in sub],
               "Step 5 -> Step 6\nMean Route Length vs Conflict Count",
               "Mean route length at T (m)")

    # 1d: box plots by event type
    type_groups = defaultdict(list)
    for r in records:
        type_groups[r["event_type"]].append(r["n_conflicts"])
    etypes = sorted(type_groups)
    bp = axes[1][1].boxplot(
        [type_groups[et] for et in etypes],
        labels=etypes, patch_artist=True, showfliers=False
    )
    type_palette = {"flood": "#1f77b4", "fire": "#d62728",
                    "wind+flood": "#ff7f0e", "structural": "#2ca02c"}
    for patch, et in zip(bp["boxes"], etypes):
        patch.set_facecolor(type_palette.get(et, "gray"))
        patch.set_alpha(0.75)
    axes[1][1].set_ylabel("Conflicts per AOI", fontsize=8)
    axes[1][1].set_title("Conflict Count by Event Type",
                         fontsize=8, fontweight="bold")
    axes[1][1].grid(axis="y", alpha=0.3, lw=0.5)
    axes[1][1].tick_params(axis="x", labelsize=7)

    # Shared legend
    handles = [plt.Line2D([0],[0], marker="o", color="w",
                          markerfacecolor=EVENT_COLORS.get(e,"gray"),
                          markersize=7, label=e)
               for e in sorted(EVENT_COLORS)]
    fig1.legend(handles=handles, fontsize=7, loc="lower center",
                ncol=6, bbox_to_anchor=(0.5, -0.02))

    plt.tight_layout(rect=[0, 0.06, 1, 0.94])
    out1 = OUT_DIR / "chain_scatter.png"
    fig1.savefig(out1, dpi=130, bbox_inches="tight")
    plt.close(fig1)
    print(f"  {out1.name}")

    # ── Plot 2: Temporal conflict rate by event type ──────────────────────────
    type_T2T3 = defaultdict(list)
    type_T3T4 = defaultdict(list)
    for a in AOI_LIST:
        et = a["event_type"]
        type_T2T3[et].append(aoi_T2T3.get(a["aoi_id"], 0))
        type_T3T4[et].append(aoi_T3T4.get(a["aoi_id"], 0))

    etypes = sorted(type_T2T3)
    x      = np.arange(len(etypes))
    w      = 0.35

    fig2, ax_t = plt.subplots(figsize=(9, 4.5))
    ax_t.bar(x - w/2,
             [np.mean(type_T2T3[et]) for et in etypes],
             width=w, label="T2->T3", color="#e07b00", alpha=0.85)
    ax_t.bar(x + w/2,
             [np.mean(type_T3T4[et]) for et in etypes],
             width=w, label="T3->T4", color="#c0392b", alpha=0.85)
    ax_t.set_xticks(x)
    ax_t.set_xticklabels(etypes, fontsize=9)
    ax_t.set_ylabel("Mean conflicts per AOI", fontsize=9)
    ax_t.set_title(
        "Step 6c — Mean Conflicts per Epoch Pair by Event Type\n"
        "T3->T4 should exceed T2->T3: damage accumulates",
        fontsize=10, fontweight="bold"
    )
    ax_t.legend(fontsize=9)
    ax_t.grid(axis="y", alpha=0.3, lw=0.5)

    plt.tight_layout()
    out2 = OUT_DIR / "conflict_temporal_by_type.png"
    fig2.savefig(out2, dpi=130, bbox_inches="tight")
    plt.close(fig2)
    print(f"  {out2.name}")

    # ── Summary ───────────────────────────────────────────────────────────────
    aois_w = [r for r in records if r["n_conflicts"] > 0]
    print(f"\nSummary:")
    print(f"  AOIs with >= 1 conflict : {len(aois_w)}/{len(records)}")
    with_imp = [r for r in records if r["pct_imp"] is not None]
    if with_imp:
        r_imp = np.corrcoef(
            [r["pct_imp"] for r in with_imp],
            [r["n_conflicts"] for r in with_imp]
        )[0, 1]
        print(f"  r(impassable, conflicts): {r_imp:.3f}")
    print(f"\nDone -- 2 images in {OUT_DIR}")


if __name__ == "__main__":
    main()
