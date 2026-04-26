"""
Step 3 Statistics — Damage + Flood Summary
-------------------------------------------
Three output images:
  1. damage_class_dist.png   — stacked bar: polygon count per damage class per event
  2. source_breakdown.png    — pie + bar: xBD vs spectral polygon counts
  3. flood_area_profile.png  — flood feature count over epoch labels per event

Run from repo root:
    python verification/step3_damage_annotation/step3_stats.py
"""

import json
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from pathlib import Path
from collections import defaultdict

BASE        = Path(__file__).resolve().parents[2]
ANN_DIR     = BASE / "data" / "annotations"
AOI_LIST    = json.load(open(BASE / "data" / "aoi_list.json"))
EPOCHS_MAP  = json.load(open(BASE / "data" / "aoi_epochs.json"))
OUT_DIR     = Path(__file__).parent / "step3_stats"
OUT_DIR.mkdir(exist_ok=True)

CLASS_COLORS = {1: "#f9d71c", 2: "#f97c1c", 3: "#e82020"}
CLASS_LABELS = {1: "Class 1\n(minor)", 2: "Class 2\n(moderate)", 3: "Class 3\n(destroyed)"}

AOI_TO_EVENT = {a["aoi_id"]: a["event_id"] for a in AOI_LIST}
AOI_TO_TYPE  = {a["aoi_id"]: a["event_type"] for a in AOI_LIST}


def gather_damage_stats():
    """Returns event_id -> {class: count, 'xbd': count, 'spectral': count}."""
    stats = defaultdict(lambda: defaultdict(int))
    for aoi_dir in sorted(ANN_DIR.iterdir()):
        if not aoi_dir.is_dir():
            continue
        aoi_id = aoi_dir.name
        eid    = AOI_TO_EVENT.get(aoi_id)
        if not eid:
            continue
        for f in aoi_dir.glob("damage_polygons_T*.geojson"):
            data = json.load(open(f))
            for feat in data.get("features", []):
                cls = feat["properties"].get("damage_class")
                src = feat["properties"].get("source", "unknown")
                if cls in (1, 2, 3):
                    stats[eid][cls] += 1
                    stats[eid][src] += 1
    return stats


def gather_flood_stats():
    """Returns event_id -> epoch_label -> feature_count."""
    stats = defaultdict(lambda: defaultdict(int))
    epoch_label_map = {}
    for aoi_id, epochs in EPOCHS_MAP.items():
        for ep in epochs:
            epoch_label_map[(aoi_id, ep["epoch_label"])] = ep

    for aoi_dir in sorted(ANN_DIR.iterdir()):
        if not aoi_dir.is_dir():
            continue
        aoi_id = aoi_dir.name
        eid    = AOI_TO_EVENT.get(aoi_id)
        if not eid:
            continue
        epochs = EPOCHS_MAP.get(aoi_id, [])
        for f in aoi_dir.glob("flood_extent_T*.geojson"):
            t_num = int(f.stem.split("_T")[1])
            lbl = epochs[t_num - 1]["epoch_label"] if t_num <= len(epochs) else "unknown"
            data = json.load(open(f))
            stats[eid][lbl] += len(data.get("features", []))
    return stats


# ── 1. Damage class distribution ─────────────────────────────────────────────

def plot_damage_class_dist(damage_stats):
    events = sorted(damage_stats.keys())
    x = np.arange(len(events))
    c1 = [damage_stats[e][1] for e in events]
    c2 = [damage_stats[e][2] for e in events]
    c3 = [damage_stats[e][3] for e in events]

    fig, ax = plt.subplots(figsize=(13, 5))
    ax.bar(x, c1, color=CLASS_COLORS[1], label="Class 1 (minor)")
    ax.bar(x, c2, bottom=c1, color=CLASS_COLORS[2], label="Class 2 (moderate)")
    ax.bar(x, c3, bottom=[a + b for a, b in zip(c1, c2)],
           color=CLASS_COLORS[3], label="Class 3 (destroyed)")

    ax.set_xticks(x)
    ax.set_xticklabels(events, rotation=30, ha="right", fontsize=9)
    ax.set_ylabel("Polygon count", fontsize=10)
    ax.set_title("Step 3 — Damage Polygon Count by Class per Event", fontsize=11, fontweight="bold")
    ax.legend(fontsize=9)
    ax.grid(axis="y", alpha=0.3, lw=0.5)

    # Annotate totals
    totals = [a + b + c for a, b, c in zip(c1, c2, c3)]
    for xi, t in zip(x, totals):
        ax.text(xi, t * 1.01, f"{t:,}", ha="center", va="bottom", fontsize=7, rotation=45)

    plt.tight_layout()
    out = OUT_DIR / "damage_class_dist.png"
    fig.savefig(out, dpi=130, bbox_inches="tight")
    plt.close(fig)
    print(f"  {out.name}")


# ── 2. Source breakdown ───────────────────────────────────────────────────────

def plot_source_breakdown(damage_stats):
    events = sorted(damage_stats.keys())
    xbd_counts      = [damage_stats[e].get("xbd", 0) for e in events]
    spectral_counts = [damage_stats[e].get("spectral", 0) for e in events]

    total_xbd      = sum(xbd_counts)
    total_spectral = sum(spectral_counts)
    total          = total_xbd + total_spectral

    fig, (ax_pie, ax_bar) = plt.subplots(1, 2, figsize=(13, 5))
    fig.suptitle("Step 3 — Damage Source Breakdown (xBD vs Spectral)", fontsize=11, fontweight="bold")

    # Pie — overall
    if total > 0:
        labels  = ["xBD ground truth", "Spectral (NDWI/NDVI)"]
        sizes   = [total_xbd, total_spectral]
        colors  = ["#2196F3", "#FF9800"]
        wedge_props = dict(width=0.5)
        ax_pie.pie(sizes, labels=labels, colors=colors, autopct="%1.1f%%",
                   startangle=90, wedgeprops=wedge_props, textprops={"fontsize": 9})
        ax_pie.set_title(f"Overall  (n={total:,} polygons)", fontsize=9)
    else:
        ax_pie.text(0.5, 0.5, "No data", ha="center", va="center",
                    transform=ax_pie.transAxes, fontsize=12)

    # Bar — per event
    x = np.arange(len(events))
    ax_bar.bar(x, spectral_counts, color="#FF9800", label="Spectral", alpha=0.85)
    ax_bar.bar(x, xbd_counts, bottom=spectral_counts, color="#2196F3", label="xBD", alpha=0.85)
    ax_bar.set_xticks(x)
    ax_bar.set_xticklabels(events, rotation=30, ha="right", fontsize=9)
    ax_bar.set_ylabel("Polygon count", fontsize=10)
    ax_bar.set_title("Per event", fontsize=9)
    ax_bar.legend(fontsize=9)
    ax_bar.grid(axis="y", alpha=0.3, lw=0.5)

    plt.tight_layout(rect=[0, 0, 1, 0.94])
    out = OUT_DIR / "source_breakdown.png"
    fig.savefig(out, dpi=130, bbox_inches="tight")
    plt.close(fig)
    print(f"  {out.name}")


# ── 3. Flood feature count profile ───────────────────────────────────────────

def plot_flood_area_profile(flood_stats):
    events      = sorted(flood_stats.keys())
    label_order = ["pre_event", "during_1", "during_2", "during_3"]
    x           = list(range(len(label_order)))
    x_labels    = ["pre", "during_1", "during_2", "during_3"]

    n_cols = 4
    n_rows = (len(events) + n_cols - 1) // n_cols

    fig, axes = plt.subplots(n_rows, n_cols,
                             figsize=(n_cols * 3.2, n_rows * 3.0),
                             squeeze=False)
    fig.suptitle(
        "Step 3 — Flood Feature Count per Epoch Label\n"
        "Counts = number of flood_extent GeoJSON features (MultiPolygon per file)",
        fontsize=10, fontweight="bold"
    )

    for idx, eid in enumerate(events):
        r, c = divmod(idx, n_cols)
        ax   = axes[r][c]
        etype = next((a["event_type"] for a in AOI_LIST if a["event_id"] == eid), "")
        counts = [flood_stats[eid].get(lbl, 0) for lbl in label_order]

        bars = ax.bar(x, counts, color=["#4a90d9", "#e07b00", "#c0392b", "#7b241c"], alpha=0.8)
        ax.set_title(f"{eid}  ({etype})", fontsize=8)
        ax.set_xticks(x)
        ax.set_xticklabels(x_labels, fontsize=7, rotation=15)
        ax.set_ylabel("Feature count", fontsize=7)
        ax.axvline(x=0.5, color="firebrick", lw=0.9, ls="--", alpha=0.5)
        ax.grid(axis="y", alpha=0.3, lw=0.5)

        for bar, cnt in zip(bars, counts):
            if cnt > 0:
                ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height(),
                        str(cnt), ha="center", va="bottom", fontsize=7)

    for idx in range(len(events), n_rows * n_cols):
        r, c = divmod(idx, n_cols)
        axes[r][c].set_visible(False)

    plt.tight_layout(rect=[0, 0, 1, 0.94])
    out = OUT_DIR / "flood_area_profile.png"
    fig.savefig(out, dpi=130, bbox_inches="tight")
    plt.close(fig)
    print(f"  {out.name}")


def check_polygon_quality():
    """
    Checks all damage and flood GeoJSON files for:
      - Empty feature lists (zero polygons)
      - Degenerate geometries (area == 0 or null coords)
      - Missing required properties (damage_class for damage files)
    Prints a summary; does not need shapely.
    """
    print("\nPolygon quality check (all AOIs)...")
    empty_damage = []
    missing_class = []
    degenerate = []
    empty_flood  = []
    n_damage_ok = n_flood_ok = 0

    for aoi_dir in sorted(ANN_DIR.iterdir()):
        if not aoi_dir.is_dir():
            continue
        aoi_id = aoi_dir.name

        # Damage files
        for f in sorted(aoi_dir.glob("damage_polygons_T*.geojson")):
            try:
                data = json.load(open(f))
            except Exception:
                continue
            feats = data.get("features", [])
            if not feats:
                empty_damage.append(f"{aoi_id}/{f.name}")
                continue
            for feat in feats:
                props = feat.get("properties", {})
                if props.get("damage_class") not in (0, 1, 2, 3):
                    missing_class.append(f"{aoi_id}/{f.name}")
                    break
                coords = feat.get("geometry", {}).get("coordinates", [])
                if not coords:
                    degenerate.append(f"{aoi_id}/{f.name}")
                    break
            else:
                n_damage_ok += 1

        # Flood files
        for f in sorted(aoi_dir.glob("flood_extent_T*.geojson")):
            try:
                data = json.load(open(f))
            except Exception:
                continue
            if not data.get("features"):
                empty_flood.append(f"{aoi_id}/{f.name}")
            else:
                n_flood_ok += 1

    print(f"  Damage files OK             : {n_damage_ok}")
    print(f"  Damage files — empty        : {len(empty_damage)}")
    print(f"  Damage files — missing class: {len(missing_class)}")
    print(f"  Damage files — degenerate   : {len(degenerate)}")
    print(f"  Flood files OK              : {n_flood_ok}")
    print(f"  Flood files — empty         : {len(empty_flood)}")

    if empty_damage:
        print(f"  Empty damage (first 10): {empty_damage[:10]}")
    if missing_class:
        print(f"  Missing damage_class (first 10): {missing_class[:10]}")
    if empty_flood:
        print(f"  Empty flood (first 10): {empty_flood[:10]}")
    if not (empty_damage or missing_class or degenerate):
        print("  All damage polygons: valid class + non-empty geometry.")


def main():
    print("Gathering damage stats...")
    damage_stats = gather_damage_stats()
    print("Gathering flood stats...")
    flood_stats  = gather_flood_stats()

    check_polygon_quality()

    print("\nPlotting...")
    plot_damage_class_dist(damage_stats)
    plot_source_breakdown(damage_stats)
    plot_flood_area_profile(flood_stats)

    # Summary to terminal
    total_polys = sum(damage_stats[e][1] + damage_stats[e][2] + damage_stats[e][3]
                      for e in damage_stats)
    print(f"\nTotal damage polygons: {total_polys:,}")
    print(f"  Class 1: {sum(damage_stats[e][1] for e in damage_stats):,}")
    print(f"  Class 2: {sum(damage_stats[e][2] for e in damage_stats):,}")
    print(f"  Class 3: {sum(damage_stats[e][3] for e in damage_stats):,}")
    xbd = sum(damage_stats[e].get('xbd', 0) for e in damage_stats)
    spec = sum(damage_stats[e].get('spectral', 0) for e in damage_stats)
    print(f"  xBD source: {xbd:,}  spectral: {spec:,}")
    print(f"\nDone — 3 stat PNGs in {OUT_DIR}")


if __name__ == "__main__":
    main()
