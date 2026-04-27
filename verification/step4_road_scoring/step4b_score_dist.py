"""
Step 4b Verification — Score Distribution + Status Breakdown
-------------------------------------------------------------
Two output images:

  1. score_distributions.png
     Box plots of road damage scores per event per epoch label.
     All roads in all AOIs for that event are pooled together.
     Expected: flood/structural events should skew higher than fire/wind.

  2. status_breakdown.png
     Stacked bar chart: passable / degraded / impassable road counts
     per event, grouped by epoch label.

Output: verification/step4_road_scoring/

Run from repo root:
    python verification/step4_road_scoring/step4b_score_dist.py
"""

import json
import sqlite3
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from collections import defaultdict
from pathlib import Path

BASE       = Path(__file__).resolve().parents[2]
ANN_DIR    = BASE / "data" / "annotations"
AOI_LIST   = json.load(open(BASE / "data" / "aoi_list.json"))
EPOCHS_MAP = json.load(open(BASE / "data" / "aoi_epochs.json"))
OUT_DIR    = Path(__file__).parent
OUT_DIR.mkdir(exist_ok=True)

LABEL_ORDER  = ["during_1", "during_2", "during_3"]
LABEL_SHORT  = ["d1", "d2", "d3"]
LABEL_COLORS = {"during_1": "#e07b00", "during_2": "#c0392b", "during_3": "#7b241c"}
STATUS_COLORS = {"passable": "#27ae60", "degraded": "#e67e22", "impassable": "#e74c3c"}


def epoch_num_from_label(aoi_id, label):
    for i, ep in enumerate(EPOCHS_MAP.get(aoi_id, [])):
        if ep["epoch_label"] == label:
            return i + 1
    return None


def read_scores_and_statuses(gpkg_path, aoi_id):
    """Returns {epoch_label: {'scores': [...], 'statuses': {...}}}"""
    conn = sqlite3.connect(gpkg_path)
    cur  = conn.cursor()
    cols = [d[0] for d in cur.execute("SELECT * FROM road_damage_scores LIMIT 0").description]
    rows = cur.execute("SELECT * FROM road_damage_scores").fetchall()
    conn.close()

    result = {}
    for lbl in LABEL_ORDER:
        ep = epoch_num_from_label(aoi_id, lbl)
        if ep is None:
            continue
        sc = f"score_T{ep}"
        st = f"status_T{ep}"
        if sc not in cols:
            continue
        si = cols.index(sc)
        ti = cols.index(st) if st in cols else None
        scores   = [r[si] for r in rows if r[si] is not None]
        statuses = {}
        if ti is not None:
            for r in rows:
                s = r[ti] or "unknown"
                statuses[s] = statuses.get(s, 0) + 1
        result[lbl] = {"scores": scores, "statuses": statuses}
    return result


def main():
    aoi_to_event = {a["aoi_id"]: a["event_id"] for a in AOI_LIST}

    # Collect: event_id -> epoch_label -> {scores, statuses}
    event_data = defaultdict(lambda: defaultdict(lambda: {"scores": [], "statuses": defaultdict(int)}))

    for a in AOI_LIST:
        aoi_id = a["aoi_id"]
        gpkg   = ANN_DIR / aoi_id / "road_damage_scores.gpkg"
        if not gpkg.exists():
            continue
        eid = a["event_id"]
        per_label = read_scores_and_statuses(gpkg, aoi_id)
        for lbl, data in per_label.items():
            event_data[eid][lbl]["scores"].extend(data["scores"])
            for s, cnt in data["statuses"].items():
                event_data[eid][lbl]["statuses"][s] += cnt

    events = sorted(event_data.keys())
    n_cols = 4
    n_rows = (len(events) + n_cols - 1) // n_cols

    # ── Plot 1: Score distributions ──────────────────────────────────────────
    fig1, axes1 = plt.subplots(n_rows, n_cols,
                               figsize=(n_cols * 3.6, n_rows * 3.2),
                               squeeze=False)
    fig1.suptitle(
        "Step 4b — Road Damage Score Distribution per Event\n"
        "Each box = all road scores for that epoch label across all AOIs",
        fontsize=10, fontweight="bold"
    )

    for idx, eid in enumerate(events):
        r, c = divmod(idx, n_cols)
        ax   = axes1[r][c]
        etype = next(a["event_type"] for a in AOI_LIST if a["event_id"] == eid)

        box_data  = []
        positions = []
        colors    = []
        for pos, lbl in enumerate(LABEL_ORDER):
            scores = event_data[eid][lbl]["scores"]
            if scores:
                box_data.append(scores)
                positions.append(pos)
                colors.append(LABEL_COLORS[lbl])

        if box_data:
            bp = ax.boxplot(box_data, positions=positions, widths=0.55,
                            patch_artist=True, showfliers=False)
            for patch, color in zip(bp["boxes"], colors):
                patch.set_facecolor(color)
                patch.set_alpha(0.75)

        ax.axhline(0.40, color="gray", lw=0.8, ls="--", alpha=0.5, label="passable/degraded")
        ax.axhline(0.60, color="gray", lw=0.8, ls=":",  alpha=0.5, label="degraded/impassable")
        ax.set_ylim(0, 1.05)
        ax.set_xticks(range(len(LABEL_ORDER)))
        ax.set_xticklabels(LABEL_SHORT, fontsize=7)
        ax.set_ylabel("Damage score", fontsize=7)
        ax.set_title(f"{eid}  ({etype})", fontsize=8)
        ax.grid(axis="y", alpha=0.3, lw=0.5)

    for idx in range(len(events), n_rows * n_cols):
        r, c = divmod(idx, n_cols)
        axes1[r][c].set_visible(False)

    plt.tight_layout(rect=[0, 0, 1, 0.94])
    out1 = OUT_DIR / "score_distributions.png"
    fig1.savefig(out1, dpi=130, bbox_inches="tight")
    plt.close(fig1)
    print(f"  {out1.name}")

    # ── Plot 2: Status breakdown ─────────────────────────────────────────────
    fig2, axes2 = plt.subplots(n_rows, n_cols,
                               figsize=(n_cols * 3.6, n_rows * 3.2),
                               squeeze=False)
    fig2.suptitle(
        "Step 4b — Road Status Breakdown per Event\n"
        "Stacked bar: passable / degraded / impassable road segments",
        fontsize=10, fontweight="bold"
    )

    for idx, eid in enumerate(events):
        r, c = divmod(idx, n_cols)
        ax   = axes2[r][c]
        etype = next(a["event_type"] for a in AOI_LIST if a["event_id"] == eid)

        xs = range(len(LABEL_ORDER))
        p_counts  = [event_data[eid][lbl]["statuses"].get("passable",   0) for lbl in LABEL_ORDER]
        d_counts  = [event_data[eid][lbl]["statuses"].get("degraded",   0) for lbl in LABEL_ORDER]
        i_counts  = [event_data[eid][lbl]["statuses"].get("impassable", 0) for lbl in LABEL_ORDER]

        ax.bar(xs, p_counts, color=STATUS_COLORS["passable"],   alpha=0.85, label="passable")
        ax.bar(xs, d_counts, bottom=p_counts,
               color=STATUS_COLORS["degraded"],   alpha=0.85, label="degraded")
        ax.bar(xs, i_counts,
               bottom=[p+d for p,d in zip(p_counts, d_counts)],
               color=STATUS_COLORS["impassable"], alpha=0.85, label="impassable")

        ax.set_xticks(list(xs))
        ax.set_xticklabels(LABEL_SHORT, fontsize=7)
        ax.set_ylabel("Road segments", fontsize=7)
        ax.set_title(f"{eid}  ({etype})", fontsize=8)
        ax.grid(axis="y", alpha=0.3, lw=0.5)

    for idx in range(len(events), n_rows * n_cols):
        r, c = divmod(idx, n_cols)
        axes2[r][c].set_visible(False)

    handles = [plt.Rectangle((0,0),1,1, color=STATUS_COLORS[s], alpha=0.85)
               for s in ["passable","degraded","impassable"]]
    fig2.legend(handles, ["Passable","Degraded","Impassable"],
                fontsize=8, loc="lower right", bbox_to_anchor=(0.99, 0.01), ncol=3)

    plt.tight_layout(rect=[0, 0.04, 1, 0.94])
    out2 = OUT_DIR / "status_breakdown.png"
    fig2.savefig(out2, dpi=130, bbox_inches="tight")
    plt.close(fig2)
    print(f"  {out2.name}")

    print(f"\nDone — 2 images in {OUT_DIR}")


if __name__ == "__main__":
    main()
