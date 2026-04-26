"""
Step 2a Verification — Download Quality
-----------------------------------------
Three output images:
  1. completeness_map.png  — AOI centroids coloured by epochs downloaded
  2. cloud_cover.png       — box plots of cloud % per event per epoch label
  3. scene_timeline.png    — dot matrix of downloaded scene dates per event

Run from repo root:
    python verification/step2_temporal_stacks/step2a_download_verify.py
"""

import json
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from pathlib import Path
from collections import defaultdict
from datetime import datetime

BASE       = Path(__file__).resolve().parents[2]
AOI_LIST   = json.load(open(BASE / "data" / "aoi_list.json"))
EPOCHS_MAP = json.load(open(BASE / "data" / "aoi_epochs.json"))
OUT_DIR    = Path(__file__).parent / "step2a_downloads"
OUT_DIR.mkdir(exist_ok=True)

LABEL_ORDER  = ["pre_event", "during_1", "during_2", "during_3"]
LABEL_COLORS = {
    "pre_event": "#4a90d9", "during_1": "#e07b00",
    "during_2":  "#c0392b", "during_3": "#7b241c",
}


# ── 1. Completeness Map ──────────────────────────────────────────────────────

def plot_completeness_map():
    by_event = defaultdict(list)
    for a in AOI_LIST:
        by_event[a["event_id"]].append(a)
    events = sorted(by_event)

    fig, axes = plt.subplots(3, 4, figsize=(16, 11))
    fig.suptitle(
        "Step 2a — Download Completeness\n"
        "Dot = AOI centroid  |  Colour = number of epochs downloaded",
        fontsize=11, fontweight="bold"
    )
    cmap = plt.cm.RdYlGn

    for idx, eid in enumerate(events):
        ax = axes[idx // 4][idx % 4]
        aois = by_event[eid]
        etype = aois[0]["event_type"]
        lons, lats, counts = [], [], []
        for a in aois:
            w, s, e, n = a["bbox"]
            lons.append((w + e) / 2)
            lats.append((s + n) / 2)
            counts.append(len(EPOCHS_MAP.get(a["aoi_id"], [])))

        sc = ax.scatter(lons, lats, c=counts, cmap=cmap, vmin=2, vmax=4,
                        s=28, edgecolors="none", alpha=0.85)
        ax.set_title(f"{eid}  ({etype})  n={len(aois)}", fontsize=8)
        ax.tick_params(labelsize=6)
        ax.set_xlabel("lon", fontsize=6)
        ax.set_ylabel("lat", fontsize=6)

    cbar = plt.colorbar(sc, ax=axes.ravel().tolist(),
                        label="Epochs downloaded",
                        orientation="horizontal", shrink=0.5, pad=0.03)
    cbar.set_ticks([2, 3, 4])
    out = OUT_DIR / "completeness_map.png"
    plt.savefig(out, dpi=130, bbox_inches="tight")
    plt.close()
    print(f"  {out.name}")


# ── 2. Cloud Cover Distribution ──────────────────────────────────────────────

def plot_cloud_cover():
    aoi_to_event = {a["aoi_id"]: a["event_id"] for a in AOI_LIST}
    data = defaultdict(lambda: defaultdict(list))
    for aoi_id, epochs in EPOCHS_MAP.items():
        eid = aoi_to_event.get(aoi_id)
        if not eid:
            continue
        for ep in epochs:
            cc = ep["cloud_cover"]
            if cc is not None:
                data[eid][ep["epoch_label"]].append(cc)

    events = sorted(data)
    fig, axes = plt.subplots(3, 4, figsize=(16, 9), sharey=False)
    fig.suptitle(
        "Step 2a — Cloud Cover Distribution per Event  (lower = better)\n"
        "Dashed line = 10% threshold used during scene selection",
        fontsize=11, fontweight="bold"
    )

    for idx, eid in enumerate(events):
        ax = axes[idx // 4][idx % 4]
        etype = next(a["event_type"] for a in AOI_LIST if a["event_id"] == eid)
        box_data = [data[eid].get(lbl, [0]) or [0] for lbl in LABEL_ORDER]
        bp = ax.boxplot(box_data, positions=range(4), widths=0.55,
                        patch_artist=True, showfliers=True,
                        flierprops=dict(marker=".", markersize=3, alpha=0.5))
        for patch, lbl in zip(bp["boxes"], LABEL_ORDER):
            patch.set_facecolor(LABEL_COLORS[lbl])
            patch.set_alpha(0.8)
        ax.set_xticks(range(4))
        ax.set_xticklabels(["pre", "d1", "d2", "d3"], fontsize=7)
        ax.set_title(f"{eid}  ({etype})", fontsize=8)
        ax.set_ylabel("Cloud %", fontsize=7)
        ax.axhline(10, color="gray", lw=0.8, ls="--", alpha=0.6)
        ax.grid(axis="y", alpha=0.3, lw=0.5)

    plt.tight_layout(rect=[0, 0, 1, 0.94])
    out = OUT_DIR / "cloud_cover.png"
    plt.savefig(out, dpi=130, bbox_inches="tight")
    plt.close()
    print(f"  {out.name}")


# ── 3. Scene Timeline ─────────────────────────────────────────────────────────

def plot_scene_timeline():
    by_event = defaultdict(list)
    for a in AOI_LIST:
        by_event[a["event_id"]].append(a)
    events = sorted(by_event)

    fig, axes = plt.subplots(len(events), 1, figsize=(14, 18))
    fig.suptitle(
        "Step 2a — Downloaded Scene Timeline\n"
        "Each dot = one epoch downloaded for one AOI  |  Colour = epoch label",
        fontsize=11, fontweight="bold"
    )

    for idx, eid in enumerate(events):
        ax = axes[idx]
        aois = by_event[eid]
        etype = aois[0]["event_type"]
        for aoi_idx, a in enumerate(aois):
            for ep in EPOCHS_MAP.get(a["aoi_id"], []):
                date  = datetime.strptime(ep["date"], "%Y%m%d")
                color = LABEL_COLORS.get(ep["epoch_label"], "gray")
                ax.scatter(date, aoi_idx, c=color, s=5, linewidths=0, alpha=0.8)
        ax.set_ylabel(f"{eid}\n({etype})", fontsize=7, rotation=0,
                      ha="right", va="center", labelpad=85)
        ax.set_yticks([])
        ax.tick_params(axis="x", labelsize=7)
        ax.grid(axis="x", alpha=0.2, lw=0.5)

    handles = [mpatches.Patch(color=LABEL_COLORS[l], label=l) for l in LABEL_ORDER]
    fig.legend(handles=handles, loc="lower right", fontsize=8,
               ncol=2, bbox_to_anchor=(0.99, 0.01))
    plt.tight_layout(rect=[0.1, 0.02, 1, 0.97])
    out = OUT_DIR / "scene_timeline.png"
    plt.savefig(out, dpi=130, bbox_inches="tight")
    plt.close()
    print(f"  {out.name}")


def print_completeness_report():
    """Prints a per-event and per-AOI completeness table to the terminal."""
    raw_dir = Path(__file__).resolve().parents[2] / "data" / "raw_scenes"
    by_event = {}
    for a in AOI_LIST:
        by_event.setdefault(a["event_id"], []).append(a)

    total_ok = total_fail = 0
    failed_aois = []
    print("\n  Event        AOIs  OK   Fail  Avg epochs")
    print("  " + "-" * 45)
    for eid in sorted(by_event):
        aois   = by_event[eid]
        n_ok = n_fail = 0
        epoch_counts = []
        for a in aois:
            n_ep = len(EPOCHS_MAP.get(a["aoi_id"], []))
            flag = raw_dir / a["aoi_id"] / "download_complete.flag"
            if flag.exists() and n_ep >= 3:
                n_ok += 1
            else:
                n_fail += 1
                failed_aois.append((a["aoi_id"], eid, n_ep))
            epoch_counts.append(n_ep)
        avg = sum(epoch_counts) / len(epoch_counts) if epoch_counts else 0
        total_ok += n_ok; total_fail += n_fail
        status = "" if n_fail == 0 else "  <-- INCOMPLETE"
        print(f"  {eid:<12} {len(aois):>4}  {n_ok:>3}  {n_fail:>4}  {avg:.2f}{status}")
    print("  " + "-" * 45)
    print(f"  TOTAL        {len(AOI_LIST):>4}  {total_ok:>3}  {total_fail:>4}")

    if failed_aois:
        print(f"\n  Failed AOIs ({len(failed_aois)}):")
        for aoi_id, eid, n_ep in sorted(failed_aois):
            print(f"    {aoi_id}  ({eid})  epochs={n_ep}")
    else:
        print("\n  All AOIs: download complete flags present and >=3 epochs.")


def main():
    print("Running Step 2a download verification...")
    print_completeness_report()
    plot_completeness_map()
    plot_cloud_cover()
    plot_scene_timeline()
    print(f"\nDone — 3 images in {OUT_DIR}")


if __name__ == "__main__":
    main()
