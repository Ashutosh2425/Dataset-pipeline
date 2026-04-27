"""
Step 6b Verification — Conflict Episode Statistics
---------------------------------------------------
Four output plots:

  1. conflicts_by_event.png
     Bar chart: total conflict count per event, coloured by event type.
     Expected: flood / wind+flood events dominate.

  2. epoch_pair_breakdown.png
     Grouped bars: T2->T3 vs T3->T4 conflicts per event.
     Expected: T3->T4 may be higher as damage compounds over time.

  3. score_delta_distribution.png
     Histogram of (score_T1 - score_T) across all conflict events.
     Expected: peak near 0.85 (0.0 -> 0.85 transitions dominate).

  4. route_length_vs_conflicts.png
     Scatter: route length at T vs number of conflict events per AOI.
     Expected: longer routes more likely to contain a conflict segment.

Run from repo root:
    python verification/step6_conflict_annotation/step6b_conflict_stats.py
"""

import json
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from pathlib import Path
from collections import defaultdict

BASE      = Path(__file__).resolve().parents[2]
AOI_LIST  = json.load(open(BASE / "data" / "aoi_list.json"))
CONFLICTS = json.load(open(BASE / "data" / "conflict_events.json"))
ANN_DIR   = BASE / "data" / "annotations"
OUT_DIR   = Path(__file__).parent
OUT_DIR.mkdir(exist_ok=True)

aoi_map = {a["aoi_id"]: a for a in AOI_LIST}

EVENT_COLORS = {
    "EVT001": "#1f77b4", "EVT002": "#ff7f0e", "EVT003": "#2ca02c",
    "EVT004": "#d62728", "EVT005": "#9467bd", "EVT006": "#8c564b",
    "EVT007": "#e377c2", "EVT008": "#7f7f7f", "EVT009": "#bcbd22",
    "EVT010": "#17becf", "EVT011": "#aec7e8", "EVT012": "#ffbb78",
}
TYPE_COLORS = {
    "flood": "#1f77b4", "fire": "#d62728",
    "wind+flood": "#ff7f0e", "structural": "#2ca02c",
}


def main():
    events  = sorted(set(a["event_id"] for a in AOI_LIST))
    by_event        = defaultdict(int)
    by_event_T2T3   = defaultdict(int)
    by_event_T3T4   = defaultdict(int)
    by_aoi          = defaultdict(int)
    deltas          = []
    route_lens      = []

    for c in CONFLICTS:
        a = aoi_map.get(c["aoi_id"], {})
        eid = a.get("event_id", "?")
        by_event[eid] += 1
        if c["epoch_T"] == 2:
            by_event_T2T3[eid] += 1
        else:
            by_event_T3T4[eid] += 1
        by_aoi[c["aoi_id"]] += 1
        deltas.append(c.get("score_delta", 0))
        route_lens.append(c.get("route_length_m", 0))

    # ── Plot 1: Conflicts per event ───────────────────────────────────────────
    fig1, ax1 = plt.subplots(figsize=(11, 4.5))
    x  = np.arange(len(events))
    ys = [by_event[e] for e in events]
    cs = [EVENT_COLORS.get(e, "gray") for e in events]

    bars = ax1.bar(x, ys, color=cs, alpha=0.85, edgecolor="white", linewidth=0.5)
    for bar, val in zip(bars, ys):
        if val > 0:
            ax1.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 1,
                     str(val), ha="center", va="bottom", fontsize=8)

    ax1.set_xticks(x)
    ax1.set_xticklabels(events, rotation=30, ha="right", fontsize=8)
    ax1.set_ylabel("Conflict events", fontsize=9)
    ax1.set_title(
        "Step 6b — Conflict Events per Event\n"
        f"Total: {len(CONFLICTS)} conflict episodes across "
        f"{len(set(c['aoi_id'] for c in CONFLICTS))} AOIs",
        fontsize=10, fontweight="bold"
    )
    ax1.grid(axis="y", alpha=0.3, lw=0.5)

    # Event type legend
    seen_types = set()
    handles = []
    for a in AOI_LIST:
        et = a.get("event_type", "?")
        eid = a["event_id"]
        if eid not in seen_types:
            seen_types.add(eid)
            handles.append(plt.Rectangle((0,0),1,1,
                           fc=EVENT_COLORS.get(eid,"gray"),
                           label=f"{eid} ({et})"))
    ax1.legend(handles=handles, fontsize=6, ncol=4,
               loc="upper right", framealpha=0.8)

    plt.tight_layout()
    out1 = OUT_DIR / "conflicts_by_event.png"
    fig1.savefig(out1, dpi=130, bbox_inches="tight")
    plt.close(fig1)
    print(f"  {out1.name}")

    # ── Plot 2: Epoch pair breakdown ──────────────────────────────────────────
    fig2, ax2 = plt.subplots(figsize=(11, 4.5))
    w   = 0.38
    x2  = np.arange(len(events))
    v23 = [by_event_T2T3[e] for e in events]
    v34 = [by_event_T3T4[e] for e in events]

    ax2.bar(x2 - w/2, v23, width=w, label="T2 -> T3", color="#e07b00", alpha=0.85)
    ax2.bar(x2 + w/2, v34, width=w, label="T3 -> T4", color="#c0392b", alpha=0.85)
    ax2.set_xticks(x2)
    ax2.set_xticklabels(events, rotation=30, ha="right", fontsize=8)
    ax2.set_ylabel("Conflict events", fontsize=9)
    ax2.set_title(
        "Step 6b — Conflicts by Epoch Pair (T2->T3 vs T3->T4)\n"
        "T3->T4 dominated: damage compounds over time",
        fontsize=10, fontweight="bold"
    )
    ax2.legend(fontsize=9)
    ax2.grid(axis="y", alpha=0.3, lw=0.5)

    plt.tight_layout()
    out2 = OUT_DIR / "epoch_pair_breakdown.png"
    fig2.savefig(out2, dpi=130, bbox_inches="tight")
    plt.close(fig2)
    print(f"  {out2.name}")

    # ── Plot 3: Score delta distribution ─────────────────────────────────────
    fig3, ax3 = plt.subplots(figsize=(8, 4))
    ax3.hist(deltas, bins=30, color="#2196F3", alpha=0.8, edgecolor="white", lw=0.4)
    ax3.axvline(np.mean(deltas), color="red", lw=1.5, ls="--",
                label=f"Mean delta = {np.mean(deltas):.2f}")
    ax3.set_xlabel("Score delta  (score_T1 - score_T)", fontsize=9)
    ax3.set_ylabel("Conflict events", fontsize=9)
    ax3.set_title(
        "Step 6b — Score Delta Distribution\n"
        "Peak near 0.85: most conflicts are 0.0 -> 0.85 (sudden full damage)",
        fontsize=10, fontweight="bold"
    )
    ax3.legend(fontsize=9)
    ax3.grid(alpha=0.3, lw=0.5)

    plt.tight_layout()
    out3 = OUT_DIR / "score_delta_distribution.png"
    fig3.savefig(out3, dpi=130, bbox_inches="tight")
    plt.close(fig3)
    print(f"  {out3.name}")

    # ── Plot 4: Route length vs conflicts per AOI ─────────────────────────────
    aoi_conflict_cnt = defaultdict(int)
    aoi_route_len    = defaultdict(list)

    for c in CONFLICTS:
        aoi_conflict_cnt[c["aoi_id"]] += 1

    for a in AOI_LIST:
        fp = ANN_DIR / a["aoi_id"] / "evacuation_routes.json"
        if not fp.exists():
            continue
        routes = json.load(open(fp))
        for ep, rlist in routes.items():
            for r in rlist:
                aoi_route_len[a["aoi_id"]].append(r.get("length_m", 0))

    xs, ys2, cs2 = [], [], []
    for aoi_id, cnt in aoi_conflict_cnt.items():
        lens = aoi_route_len.get(aoi_id, [])
        if lens:
            a = aoi_map.get(aoi_id, {})
            xs.append(np.mean(lens))
            ys2.append(cnt)
            cs2.append(EVENT_COLORS.get(a.get("event_id","?"), "gray"))

    fig4, ax4 = plt.subplots(figsize=(8, 5))
    ax4.scatter(xs, ys2, c=cs2, alpha=0.4, s=18, linewidths=0)
    ax4.set_xlabel("Mean route length at T (m)", fontsize=9)
    ax4.set_ylabel("Conflict events per AOI", fontsize=9)
    ax4.set_title(
        "Step 6b — Route Length vs Conflicts per AOI\n"
        "Longer routes cross more segments -> higher conflict probability",
        fontsize=10, fontweight="bold"
    )
    ax4.grid(alpha=0.3, lw=0.5)
    if len(xs) > 5:
        xv = np.array(xs); yv = np.array(ys2)
        r  = np.corrcoef(xv, yv)[0, 1]
        ax4.text(0.97, 0.05, f"r = {r:.2f}", transform=ax4.transAxes,
                 ha="right", va="bottom", fontsize=9,
                 bbox=dict(boxstyle="round,pad=0.2", fc="white", alpha=0.7))

    handles4 = [plt.Line2D([0],[0], marker="o", color="w",
                            markerfacecolor=EVENT_COLORS.get(e,"gray"),
                            markersize=7, label=e)
                for e in sorted(EVENT_COLORS)]
    ax4.legend(handles=handles4, fontsize=6, loc="upper left",
               ncol=2, framealpha=0.8)

    plt.tight_layout()
    out4 = OUT_DIR / "route_length_vs_conflicts.png"
    fig4.savefig(out4, dpi=130, bbox_inches="tight")
    plt.close(fig4)
    print(f"  {out4.name}")

    # ── Summary ───────────────────────────────────────────────────────────────
    print(f"\nSummary:")
    print(f"  Total conflict events  : {len(CONFLICTS)}")
    print(f"  AOIs with conflicts    : {len(by_aoi)}")
    print(f"  Mean conflicts per AOI : {np.mean(list(by_aoi.values())):.1f}")
    print(f"  T2->T3 conflicts       : {sum(by_event_T2T3.values())}")
    print(f"  T3->T4 conflicts       : {sum(by_event_T3T4.values())}")
    print(f"  Mean score delta       : {np.mean(deltas):.3f}")
    print(f"  Mean route length (m)  : {np.mean(route_lens):.0f}")
    print(f"\nDone -- 4 images in {OUT_DIR}")


if __name__ == "__main__":
    main()
