"""
Step 5b Verification — Evacuation Route Statistics
---------------------------------------------------
Four output images:

  1. length_distribution.png
     Box plots of route length (m) per event per epoch.
     Expected: longer routes in flood/wind events where direct paths are blocked.

  2. eta_distribution.png
     Box plots of ETA (minutes walk) per event per epoch.
     Cross-check: ETA = length / (5 km/h walking speed).

  3. routes_per_aoi.png
     Histogram + per-event bar of routes found per AOI per epoch.
     AOIs with 0 routes = no OSM road graph (expected in rural areas).

  4. shelter_type_breakdown.png
     Bar chart: how many routes go to each shelter type.
     Shows whether real OSM amenities or synthetic intersections dominate.

Run from repo root:
    python verification/step5_evacuation_routes/step5b_route_stats.py
"""

import json
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from pathlib import Path
from collections import defaultdict

BASE      = Path(__file__).resolve().parents[2]
ANN_DIR   = BASE / "data" / "annotations"
AOI_LIST  = json.load(open(BASE / "data" / "aoi_list.json"))
OUT_DIR   = Path(__file__).parent
OUT_DIR.mkdir(exist_ok=True)

EPOCH_LABELS  = ["T2", "T3", "T4"]
EPOCH_COLORS  = {"T2": "#e07b00", "T3": "#c0392b", "T4": "#7b241c"}
EVENT_COLORS  = {
    "EVT001": "#1f77b4", "EVT002": "#ff7f0e", "EVT003": "#2ca02c",
    "EVT004": "#d62728", "EVT005": "#9467bd", "EVT006": "#8c564b",
    "EVT007": "#e377c2", "EVT008": "#7f7f7f", "EVT009": "#bcbd22",
    "EVT010": "#17becf", "EVT011": "#aec7e8", "EVT012": "#ffbb78",
}


def load_all_routes():
    """
    Returns list of dicts, one per route:
      aoi_id, event_id, event_type, epoch, route_id,
      shelter_name, shelter_type, length_m, eta_min, rss
    """
    aoi_map = {a["aoi_id"]: a for a in AOI_LIST}
    records = []
    for a in AOI_LIST:
        aoi_id = a["aoi_id"]
        fp = ANN_DIR / aoi_id / "evacuation_routes.json"
        if not fp.exists():
            continue
        routes = json.load(open(fp))
        for epoch, rlist in routes.items():
            for i, r in enumerate(rlist, 1):
                sname = r.get("shelter_name", "")
                # Classify shelter type
                if "synthetic" in sname.lower() or "intersection" in sname.lower():
                    stype = "synthetic_intersection"
                elif any(k in sname.lower() for k in ["school", "university", "college"]):
                    stype = "school"
                elif any(k in sname.lower() for k in ["hospital", "clinic", "health"]):
                    stype = "hospital"
                elif any(k in sname.lower() for k in ["church", "mosque", "temple"]):
                    stype = "religious"
                elif any(k in sname.lower() for k in ["community", "centre", "center", "hall"]):
                    stype = "community_center"
                else:
                    stype = "other_osm"
                records.append({
                    "aoi_id":      aoi_id,
                    "event_id":    a["event_id"],
                    "event_type":  a["event_type"],
                    "epoch":       epoch,
                    "route_id":    i,
                    "shelter_name": sname,
                    "shelter_type": stype,
                    "length_m":    r.get("length_m", 0),
                    "eta_min":     r.get("eta_min", 0),
                    "rss":         r.get("rss", 0),
                })
    return records


def load_routes_per_aoi():
    """Returns (aoi_id, event_id, epoch) -> route_count."""
    aoi_map  = {a["aoi_id"]: a for a in AOI_LIST}
    counts   = defaultdict(int)
    for a in AOI_LIST:
        for ep in EPOCH_LABELS:
            counts[(a["aoi_id"], a["event_id"], ep)] = 0  # init to 0
    for a in AOI_LIST:
        fp = ANN_DIR / a["aoi_id"] / "evacuation_routes.json"
        if not fp.exists():
            continue
        routes = json.load(open(fp))
        for epoch, rlist in routes.items():
            counts[(a["aoi_id"], a["event_id"], epoch)] = len(rlist)
    return counts


def main():
    print("Loading route data...")
    records = load_all_routes()
    rpa     = load_routes_per_aoi()
    print(f"  {len(records)} routes across {len(AOI_LIST)} AOIs")

    events  = sorted(set(r["event_id"] for r in records))
    n_cols  = 4
    n_rows  = (len(events) + n_cols - 1) // n_cols

    # ── 1: Route length distribution ─────────────────────────────────────────
    fig1, axes1 = plt.subplots(n_rows, n_cols,
                               figsize=(n_cols * 3.6, n_rows * 3.2),
                               squeeze=False)
    fig1.suptitle(
        "Step 5b — Route Length Distribution per Event  (all routes, all AOIs)\n"
        "Each box = route lengths for that epoch across all AOIs in event",
        fontsize=10, fontweight="bold"
    )
    for idx, eid in enumerate(events):
        r, c = divmod(idx, n_cols)
        ax   = axes1[r][c]
        etype = next(a["event_type"] for a in AOI_LIST if a["event_id"] == eid)

        box_data, positions, colors = [], [], []
        for pos, lbl in enumerate(EPOCH_LABELS):
            vals = [x["length_m"] for x in records
                    if x["event_id"] == eid and x["epoch"] == lbl]
            if vals:
                box_data.append(vals)
                positions.append(pos)
                colors.append(EPOCH_COLORS[lbl])

        if box_data:
            bp = ax.boxplot(box_data, positions=positions, widths=0.55,
                            patch_artist=True, showfliers=False)
            for patch, col in zip(bp["boxes"], colors):
                patch.set_facecolor(col); patch.set_alpha(0.75)
            # Mean line
            for pos, vals in zip(positions, box_data):
                ax.scatter(pos, np.mean(vals), marker="D", s=18,
                           color="white", zorder=5)

        ax.set_xticks(range(len(EPOCH_LABELS)))
        ax.set_xticklabels(EPOCH_LABELS, fontsize=7)
        ax.set_ylabel("Length (m)", fontsize=7)
        ax.set_title(f"{eid}  ({etype})", fontsize=8)
        ax.grid(axis="y", alpha=0.3, lw=0.5)

    for idx in range(len(events), n_rows * n_cols):
        r, c = divmod(idx, n_cols)
        axes1[r][c].set_visible(False)

    plt.tight_layout(rect=[0, 0, 1, 0.94])
    out1 = OUT_DIR / "length_distribution.png"
    fig1.savefig(out1, dpi=130, bbox_inches="tight")
    plt.close(fig1)
    print(f"  {out1.name}")

    # ── 2: ETA distribution ───────────────────────────────────────────────────
    fig2, axes2 = plt.subplots(n_rows, n_cols,
                               figsize=(n_cols * 3.6, n_rows * 3.2),
                               squeeze=False)
    fig2.suptitle(
        "Step 5b — ETA Distribution per Event  (walking @ 5 km/h)\n"
        "Expected: >30 min ETA may indicate shelter is too far or route detoured",
        fontsize=10, fontweight="bold"
    )
    for idx, eid in enumerate(events):
        r, c = divmod(idx, n_cols)
        ax   = axes2[r][c]
        etype = next(a["event_type"] for a in AOI_LIST if a["event_id"] == eid)

        box_data, positions, colors = [], [], []
        for pos, lbl in enumerate(EPOCH_LABELS):
            vals = [x["eta_min"] for x in records
                    if x["event_id"] == eid and x["epoch"] == lbl]
            if vals:
                box_data.append(vals)
                positions.append(pos)
                colors.append(EPOCH_COLORS[lbl])

        if box_data:
            bp = ax.boxplot(box_data, positions=positions, widths=0.55,
                            patch_artist=True, showfliers=False)
            for patch, col in zip(bp["boxes"], colors):
                patch.set_facecolor(col); patch.set_alpha(0.75)

        ax.axhline(30, color="red", lw=0.8, ls="--", alpha=0.6,
                   label=">30 min concern")
        ax.set_xticks(range(len(EPOCH_LABELS)))
        ax.set_xticklabels(EPOCH_LABELS, fontsize=7)
        ax.set_ylabel("ETA (min)", fontsize=7)
        ax.set_title(f"{eid}  ({etype})", fontsize=8)
        ax.grid(axis="y", alpha=0.3, lw=0.5)

    for idx in range(len(events), n_rows * n_cols):
        r, c = divmod(idx, n_cols)
        axes2[r][c].set_visible(False)

    plt.tight_layout(rect=[0, 0, 1, 0.94])
    out2 = OUT_DIR / "eta_distribution.png"
    fig2.savefig(out2, dpi=130, bbox_inches="tight")
    plt.close(fig2)
    print(f"  {out2.name}")

    # ── 3: Routes per AOI per epoch ───────────────────────────────────────────
    fig3, axes3 = plt.subplots(n_rows, n_cols,
                               figsize=(n_cols * 3.6, n_rows * 3.2),
                               squeeze=False)
    fig3.suptitle(
        "Step 5b — Routes Found per AOI per Epoch\n"
        "0 routes = no OSM road graph or no reachable shelter",
        fontsize=10, fontweight="bold"
    )
    for idx, eid in enumerate(events):
        r, c = divmod(idx, n_cols)
        ax   = axes3[r][c]
        etype = next(a["event_type"] for a in AOI_LIST if a["event_id"] == eid)

        for pos, lbl in enumerate(EPOCH_LABELS):
            vals = [v for (aid, ev, ep), v in rpa.items()
                    if ev == eid and ep == lbl]
            if not vals:
                continue
            counts_arr = np.array(vals)
            zero_pct = (counts_arr == 0).mean() * 100
            ax.bar(pos, np.mean(counts_arr), color=EPOCH_COLORS[lbl],
                   alpha=0.8, width=0.55)
            ax.text(pos, np.mean(counts_arr) + 0.05,
                    f"0:{zero_pct:.0f}%", ha="center", fontsize=6, va="bottom")

        ax.set_xticks(range(len(EPOCH_LABELS)))
        ax.set_xticklabels(EPOCH_LABELS, fontsize=7)
        ax.set_ylabel("Mean routes/AOI", fontsize=7)
        ax.set_title(f"{eid}  ({etype})", fontsize=8)
        ax.grid(axis="y", alpha=0.3, lw=0.5)

    for idx in range(len(events), n_rows * n_cols):
        r, c = divmod(idx, n_cols)
        axes3[r][c].set_visible(False)

    plt.tight_layout(rect=[0, 0, 1, 0.94])
    out3 = OUT_DIR / "routes_per_aoi.png"
    fig3.savefig(out3, dpi=130, bbox_inches="tight")
    plt.close(fig3)
    print(f"  {out3.name}")

    # ── 4: Shelter type breakdown ─────────────────────────────────────────────
    stype_counts = defaultdict(int)
    for r in records:
        stype_counts[r["shelter_type"]] += 1

    stype_event = defaultdict(lambda: defaultdict(int))
    for r in records:
        stype_event[r["event_id"]][r["shelter_type"]] += 1

    fig4, (ax4a, ax4b) = plt.subplots(1, 2, figsize=(13, 5))
    fig4.suptitle("Step 5b — Shelter Type Breakdown\n"
                  "Synthetic intersections used when no OSM amenity exists in AOI",
                  fontsize=10, fontweight="bold")

    # Left: overall pie
    labels = list(stype_counts.keys())
    sizes  = [stype_counts[l] for l in labels]
    wedge_colors = ["#2196F3","#4CAF50","#FF9800","#E91E63","#9C27B0","#607D8B"]
    ax4a.pie(sizes, labels=labels, autopct="%1.1f%%",
             colors=wedge_colors[:len(labels)], startangle=140,
             textprops={"fontsize": 8})
    ax4a.set_title("All events combined", fontsize=9)

    # Right: stacked bar per event
    stypes = sorted(stype_counts.keys())
    x      = np.arange(len(events))
    bottoms = np.zeros(len(events))
    for si, stype in enumerate(stypes):
        vals = [stype_event[eid].get(stype, 0) for eid in events]
        ax4b.bar(x, vals, bottom=bottoms, label=stype,
                 color=wedge_colors[si % len(wedge_colors)], alpha=0.85)
        bottoms += np.array(vals, dtype=float)

    ax4b.set_xticks(x)
    ax4b.set_xticklabels(events, rotation=45, ha="right", fontsize=7)
    ax4b.set_ylabel("Route count", fontsize=8)
    ax4b.set_title("Per event", fontsize=9)
    ax4b.legend(fontsize=7, loc="upper left")
    ax4b.grid(axis="y", alpha=0.3, lw=0.5)

    plt.tight_layout(rect=[0, 0, 1, 0.93])
    out4 = OUT_DIR / "shelter_type_breakdown.png"
    fig4.savefig(out4, dpi=130, bbox_inches="tight")
    plt.close(fig4)
    print(f"  {out4.name}")

    # ── Print summary stats ───────────────────────────────────────────────────
    print(f"\nSummary:")
    print(f"  Total routes     : {len(records)}")
    print(f"  Mean length      : {np.mean([r['length_m'] for r in records]):.0f} m")
    print(f"  Mean ETA         : {np.mean([r['eta_min'] for r in records]):.1f} min")
    rss_vals = [r['rss'] for r in records]
    print(f"  RSS=1.0          : {sum(1 for v in rss_vals if v==1.0)}/{len(rss_vals)}")
    print(f"  Shelter types    : {dict(stype_counts)}")
    print(f"\nDone — 4 images in {OUT_DIR}")


if __name__ == "__main__":
    main()
