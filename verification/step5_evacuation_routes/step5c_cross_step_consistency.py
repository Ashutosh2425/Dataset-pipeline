"""
Step 5c Verification — Cross-Step Consistency (Steps 3 → 4 → 5)
-----------------------------------------------------------------
Checks whether evacuation routes respond sensibly to upstream damage signals:

  Chain verified:
    Flood extent area  (Step 3)
         ↓  more flood → longer detour
    % Impassable roads (Step 4)
         ↓  more blocked roads → fewer routes found
    Route length / ETA (Step 5)
         ↓  longer routes → higher detour factor vs straight-line

  Four scatter plots (each point = one AOI × one epoch):
    1. % impassable roads  vs  mean route length
    2. Flood extent (km²)  vs  mean route ETA
    3. % impassable roads  vs  routes found per AOI (should decrease)
    4. Detour factor        vs  % impassable roads
       (detour = actual_length / straight-line origin→shelter distance)

  Plus one temporal plot:
    Mean route length per event per epoch (does it grow post-disaster?)

Output: verification/step5_evacuation_routes/step5c_consistency/

Run from repo root:
    python verification/step5_evacuation_routes/step5c_cross_step_consistency.py
"""

import json
import sqlite3
import math
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
OUT_DIR    = Path(__file__).parent / "step5c_consistency"
OUT_DIR.mkdir(exist_ok=True)

EPOCH_LABELS = ["T2", "T3", "T4"]
DOWNSAMPLE   = 4
NDWI_THRESH  = 0.2

EVENT_COLORS = {
    "EVT001": "#1f77b4", "EVT002": "#ff7f0e", "EVT003": "#2ca02c",
    "EVT004": "#d62728", "EVT005": "#9467bd", "EVT006": "#8c564b",
    "EVT007": "#e377c2", "EVT008": "#7f7f7f", "EVT009": "#bcbd22",
    "EVT010": "#17becf", "EVT011": "#aec7e8", "EVT012": "#ffbb78",
}

GPKG_HEADER_SIZES = {0: 0, 1: 32, 2: 48, 3: 48, 4: 64}


def haversine_m(lon1, lat1, lon2, lat2):
    R = 6_371_000
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlam = math.radians(lon2 - lon1)
    a = math.sin(dphi/2)**2 + math.cos(phi1)*math.cos(phi2)*math.sin(dlam/2)**2
    return 2 * R * math.asin(math.sqrt(a))


def flood_area_km2(ann_dir, ep_num):
    path = ann_dir / f"flood_extent_T{ep_num}.geojson"
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


def road_pct_impassable(gpkg_path, ep_num):
    if not gpkg_path.exists():
        return None
    score_col  = f"score_T{ep_num}"
    status_col = f"status_T{ep_num}"
    try:
        con  = sqlite3.connect(gpkg_path)
        cols = [d[0] for d in con.execute(
            "SELECT * FROM road_damage_scores LIMIT 0").description]
        if status_col not in cols:
            con.close()
            return None
        si   = cols.index(status_col)
        rows = con.execute("SELECT * FROM road_damage_scores").fetchall()
        con.close()
        if not rows:
            return None
        return sum(1 for r in rows if r[si] == "impassable") / len(rows)
    except Exception:
        return None


def route_metrics_for_epoch(routes_json, epoch_label):
    """
    Returns (mean_length_m, mean_eta_min, n_routes, mean_detour_factor).
    detour_factor = actual_length / haversine(origin, shelter).
    """
    rlist = routes_json.get(epoch_label, [])
    if not rlist:
        return None, None, 0, None

    lengths = []
    etas    = []
    detours = []
    for r in rlist:
        lengths.append(r.get("length_m", 0))
        etas.append(r.get("eta_min", 0))
        coords = r.get("geometry", {}).get("coordinates", [])
        if len(coords) >= 2:
            d_straight = haversine_m(
                coords[0][0], coords[0][1],
                coords[-1][0], coords[-1][1])
            if d_straight > 10:
                detours.append(r.get("length_m", 0) / d_straight)

    return (np.mean(lengths) if lengths else None,
            np.mean(etas)    if etas    else None,
            len(rlist),
            np.mean(detours) if detours else None)


def epoch_num(label):
    return int(label[1])   # T2->2, T3->3, T4->4


def main():
    print("Collecting per-AOI per-epoch cross-step metrics...")

    records = []
    for a in AOI_LIST:
        aoi_id    = a["aoi_id"]
        event_id  = a["event_id"]
        event_type = a["event_type"]
        gpkg_path = ANN_DIR / aoi_id / "road_damage_scores.gpkg"
        rjp       = ANN_DIR / aoi_id / "evacuation_routes.json"

        routes_json = json.load(open(rjp)) if rjp.exists() else {}

        for lbl in EPOCH_LABELS:
            ep = epoch_num(lbl)

            fl_km2  = flood_area_km2(ANN_DIR / aoi_id, ep)
            pct_imp = road_pct_impassable(gpkg_path, ep)
            mean_len, mean_eta, n_routes, detour = route_metrics_for_epoch(
                routes_json, lbl)

            records.append({
                "aoi_id":     aoi_id,
                "event_id":   event_id,
                "event_type": event_type,
                "epoch":      lbl,
                "flood_km2":  fl_km2,
                "pct_imp":    pct_imp,
                "mean_len":   mean_len,
                "mean_eta":   mean_eta,
                "n_routes":   n_routes,
                "detour":     detour,
            })

        if len(records) % 300 == 0:
            print(f"  {len(records)//len(EPOCH_LABELS)} / {len(AOI_LIST)} AOIs...")

    print(f"  Done. {len(records)} records total.")

    def scatter_ax(ax, xs, ys, eids, title, xlabel, ylabel, hline=None):
        colors = [EVENT_COLORS.get(e, "gray") for e in eids]
        ax.scatter(xs, ys, c=colors, alpha=0.35, s=10, linewidths=0)
        if hline is not None:
            ax.axhline(hline, color="red", lw=0.8, ls="--", alpha=0.6)
        ax.set_xlabel(xlabel, fontsize=8)
        ax.set_ylabel(ylabel, fontsize=8)
        ax.set_title(title, fontsize=8, fontweight="bold")
        ax.grid(alpha=0.3, lw=0.5)
        xv = np.array(xs, dtype=float)
        yv = np.array(ys, dtype=float)
        mask = np.isfinite(xv) & np.isfinite(yv)
        if mask.sum() > 5:
            r = np.corrcoef(xv[mask], yv[mask])[0, 1]
            ax.text(0.97, 0.05, f"r = {r:.2f}", transform=ax.transAxes,
                    ha="right", va="bottom", fontsize=8,
                    bbox=dict(boxstyle="round,pad=0.2", fc="white", alpha=0.7))

    # ── Plot 1: Four scatter plots ────────────────────────────────────────────
    fig1, axes = plt.subplots(2, 2, figsize=(12, 10))
    fig1.suptitle(
        "Step 5c — Cross-Step Consistency: Damage/Flood  →  Route Quality\n"
        "Each point = one AOI x one epoch.  Colour = event.  r = Pearson correlation.",
        fontsize=10, fontweight="bold"
    )

    # 1a: % impassable vs mean route length
    mask1 = [r["pct_imp"] is not None and r["mean_len"] is not None
             for r in records]
    scatter_ax(
        axes[0][0],
        xs=[r["pct_imp"]  for r in records if r["pct_imp"] is not None and r["mean_len"] is not None],
        ys=[r["mean_len"] for r in records if r["pct_imp"] is not None and r["mean_len"] is not None],
        eids=[r["event_id"] for r in records if r["pct_imp"] is not None and r["mean_len"] is not None],
        title="Step 4 → Step 5\n% Impassable Roads vs Mean Route Length",
        xlabel="% impassable roads (Step 4)",
        ylabel="Mean route length (m)",
    )

    # 1b: Flood extent vs mean ETA
    scatter_ax(
        axes[0][1],
        xs=[r["flood_km2"] for r in records if r["mean_eta"] is not None],
        ys=[r["mean_eta"]  for r in records if r["mean_eta"] is not None],
        eids=[r["event_id"] for r in records if r["mean_eta"] is not None],
        title="Step 3 → Step 5\nFlood Extent vs Mean Route ETA",
        xlabel="Flood extent area (km²)",
        ylabel="Mean route ETA (min)",
        hline=30,
    )

    # 1c: % impassable vs routes found
    scatter_ax(
        axes[1][0],
        xs=[r["pct_imp"]  for r in records if r["pct_imp"] is not None],
        ys=[r["n_routes"] for r in records if r["pct_imp"] is not None],
        eids=[r["event_id"] for r in records if r["pct_imp"] is not None],
        title="Step 4 → Step 5\n% Impassable Roads vs Routes Found per AOI",
        xlabel="% impassable roads (Step 4)",
        ylabel="Routes found per AOI",
    )

    # 1d: Detour factor vs % impassable
    scatter_ax(
        axes[1][1],
        xs=[r["pct_imp"] for r in records
            if r["pct_imp"] is not None and r["detour"] is not None],
        ys=[r["detour"]  for r in records
            if r["pct_imp"] is not None and r["detour"] is not None],
        eids=[r["event_id"] for r in records
              if r["pct_imp"] is not None and r["detour"] is not None],
        title="End-to-End: Step 4 → Step 5\n% Impassable Roads vs Route Detour Factor",
        xlabel="% impassable roads",
        ylabel="Detour factor  (actual / straight-line)",
        hline=1.0,
    )

    # Shared event legend
    handles = [plt.Line2D([0], [0], marker="o", color="w",
                          markerfacecolor=EVENT_COLORS.get(e, "gray"),
                          markersize=7, label=e)
               for e in sorted(EVENT_COLORS)]
    fig1.legend(handles=handles, fontsize=7, loc="lower center",
                ncol=6, bbox_to_anchor=(0.5, -0.02))

    plt.tight_layout(rect=[0, 0.06, 1, 0.94])
    out1 = OUT_DIR / "chain_scatter.png"
    fig1.savefig(out1, dpi=130, bbox_inches="tight")
    plt.close(fig1)
    print(f"  {out1.name}")

    # ── Plot 2: Temporal route length per event ───────────────────────────────
    event_len = defaultdict(lambda: defaultdict(list))
    for r in records:
        if r["mean_len"] is not None:
            event_len[r["event_id"]][r["epoch"]].append(r["mean_len"])

    events  = sorted(event_len.keys())
    n_cols  = 4
    n_rows2 = (len(events) + n_cols - 1) // n_cols
    x       = list(range(len(EPOCH_LABELS)))

    fig2, axes2 = plt.subplots(n_rows2, n_cols,
                               figsize=(n_cols * 3.6, n_rows2 * 3.0),
                               squeeze=False)
    fig2.suptitle(
        "Step 5c — Mean Route Length per Event Across Epochs\n"
        "Flood/structural events should show longer routes in earlier post-event epochs",
        fontsize=10, fontweight="bold"
    )

    for idx, eid in enumerate(events):
        r, c = divmod(idx, n_cols)
        ax   = axes2[r][c]
        etype = next(a["event_type"] for a in AOI_LIST if a["event_id"] == eid)
        color = EVENT_COLORS.get(eid, "steelblue")

        means = []
        stds  = []
        for lbl in EPOCH_LABELS:
            vals = event_len[eid][lbl]
            means.append(np.mean(vals) if vals else np.nan)
            stds.append(np.std(vals)   if vals else 0)

        ax.plot(x, means, color=color, lw=2.5, marker="o", markersize=7, zorder=3)
        ax.fill_between(x,
                        [m-s for m,s in zip(means, stds)],
                        [m+s for m,s in zip(means, stds)],
                        alpha=0.15, color=color)
        ax.set_xticks(x)
        ax.set_xticklabels(EPOCH_LABELS, fontsize=7)
        ax.set_ylabel("Mean route length (m)", fontsize=7)
        ax.set_title(f"{eid}  ({etype})", fontsize=8)
        ax.grid(alpha=0.3, lw=0.5)

    for idx in range(len(events), n_rows2 * n_cols):
        r, c = divmod(idx, n_cols)
        axes2[r][c].set_visible(False)

    plt.tight_layout(rect=[0, 0, 1, 0.94])
    out2 = OUT_DIR / "route_length_temporal.png"
    fig2.savefig(out2, dpi=130, bbox_inches="tight")
    plt.close(fig2)
    print(f"  {out2.name}")

    # ── Print summary ─────────────────────────────────────────────────────────
    with_routes = [r for r in records if r["n_routes"] > 0]
    no_routes   = [r for r in records if r["n_routes"] == 0]
    print(f"\nSummary:")
    print(f"  AOI-epoch pairs   : {len(records)}")
    print(f"  With routes       : {len(with_routes)} ({len(with_routes)/len(records)*100:.1f}%)")
    print(f"  No routes (no OSM): {len(no_routes)} ({len(no_routes)/len(records)*100:.1f}%)")
    with_imp = [r for r in records if r["pct_imp"] is not None]
    if with_imp:
        print(f"  Mean % impassable : {np.mean([r['pct_imp'] for r in with_imp])*100:.1f}%")
    with_det = [r for r in records if r["detour"] is not None]
    if with_det:
        print(f"  Mean detour factor: {np.mean([r['detour'] for r in with_det]):.2f}x")

    print(f"\nDone — 2 images in {OUT_DIR}")


if __name__ == "__main__":
    main()
