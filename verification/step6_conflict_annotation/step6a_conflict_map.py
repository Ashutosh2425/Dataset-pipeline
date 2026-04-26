"""
Step 6a Verification — Conflict Episode Maps on RGB Imagery
-----------------------------------------------------------
For 2 AOIs per event (those with the most conflict events), overlays:
  - Evacuation route lines for epoch T  (blue/orange/green)
  - Conflicting road segment centroid   (red X marker)
  - RGB Sentinel-2 background for epoch T and T+1

Expected: the red X should fall on a visibly damaged road corridor
that is passable at T but destroyed/flooded by T+1.

Output: verification/step6_conflict_annotation/step6a_maps/EVT{N}_conflicts.png

Run from repo root:
    python verification/step6_conflict_annotation/step6a_conflict_map.py
"""

import json
import numpy as np
import rasterio
from rasterio.warp import transform_bounds
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.lines as mlines
import matplotlib.patches as mpatches
from pathlib import Path
from collections import defaultdict

BASE       = Path(__file__).resolve().parents[2]
STACKS_DIR = BASE / "data" / "stacks"
ANN_DIR    = BASE / "data" / "annotations"
AOI_LIST   = json.load(open(BASE / "data" / "aoi_list.json"))
CONFLICTS  = json.load(open(BASE / "data" / "conflict_events.json"))
OUT_DIR    = Path(__file__).parent / "step6a_maps"
OUT_DIR.mkdir(exist_ok=True)

N_PER_EVENT  = 2
ROUTE_COLORS = ["#2196F3", "#FF9800", "#4CAF50", "#9C27B0", "#FF5722"]


def percentile_stretch(arr, lo=2, hi=98):
    valid = arr[arr > 0]
    if valid.size == 0:
        return np.zeros_like(arr, dtype=np.float32)
    vlo, vhi = np.percentile(valid, lo), np.percentile(valid, hi)
    if vhi <= vlo:
        return np.zeros_like(arr, dtype=np.float32)
    return np.clip((arr - vlo) / (vhi - vlo), 0, 1).astype(np.float32)


def read_rgb(tif_path, epoch_idx, bpe):
    offset = sum(bpe[:epoch_idx])
    with rasterio.open(tif_path) as src:
        b = src.read(offset + 1).astype(np.float32)
        g = src.read(offset + 2).astype(np.float32)
        r = src.read(offset + 3).astype(np.float32)
        bounds_wgs84 = transform_bounds(src.crs, "EPSG:4326", *src.bounds)
    rgb = np.stack([percentile_stretch(r), percentile_stretch(g),
                    percentile_stretch(b)], axis=-1)
    return rgb, bounds_wgs84


def select_sample():
    """2 AOIs per event with the most conflict events and a valid stack TIF."""
    conflict_count = defaultdict(int)
    for c in CONFLICTS:
        conflict_count[c["aoi_id"]] += 1

    by_event = defaultdict(list)
    for a in AOI_LIST:
        by_event[a["event_id"]].append(a)

    selected = []
    for eid in sorted(by_event):
        candidates = []
        for a in by_event[eid]:
            n = conflict_count.get(a["aoi_id"], 0)
            if n == 0:
                continue
            tif  = STACKS_DIR / f"{a['aoi_id']}_stack.tif"
            meta = STACKS_DIR / f"{a['aoi_id']}_meta.json"
            rjp  = ANN_DIR / a["aoi_id"] / "evacuation_routes.json"
            if tif.exists() and meta.exists() and rjp.exists():
                candidates.append((a, json.load(open(meta)), json.load(open(rjp)), n))
        candidates.sort(key=lambda x: -x[3])
        selected.extend(candidates[:N_PER_EVENT])
    return selected


def main():
    sample   = select_sample()
    by_event = defaultdict(list)
    for item in sample:
        by_event[item[0]["event_id"]].append(item)

    conflict_by_aoi = defaultdict(list)
    for c in CONFLICTS:
        conflict_by_aoi[c["aoi_id"]].append(c)

    for event_id, items in sorted(by_event.items()):
        event_type = items[0][0]["event_type"]
        n_rows = len(items)
        n_cols = 2   # epoch T  |  epoch T+1

        fig, axes = plt.subplots(n_rows, n_cols,
                                 figsize=(n_cols * 5.0, n_rows * 4.5),
                                 squeeze=False)
        fig.suptitle(
            f"{event_id}  ({event_type})  —  Step 6: Conflict Episodes\n"
            "Left = epoch T (route safe)    Right = epoch T+1 (route invalidated)\n"
            "Blue/Orange/Green = routes    Red X = conflicting segment",
            fontsize=9, fontweight="bold"
        )

        for row, (aoi, meta, routes, n_conflicts) in enumerate(items):
            aoi_id = aoi["aoi_id"]
            tif    = STACKS_DIR / f"{aoi_id}_stack.tif"
            bpe    = meta["bands_per_epoch"]
            n_ep   = meta["n_epochs"]

            # Use first conflict to determine epoch pair
            aoi_conflicts = conflict_by_aoi.get(aoi_id, [])
            if not aoi_conflicts:
                continue
            ep_T  = aoi_conflicts[0]["epoch_T"]
            ep_T1 = aoi_conflicts[0]["epoch_T1"]

            for col, ep in enumerate([ep_T, ep_T1]):
                ax     = axes[row][col]
                ep_idx = ep - 1  # 0-based index

                if ep_idx >= n_ep:
                    ax.set_facecolor("#111")
                    ax.text(0.5, 0.5, f"T{ep} not available",
                            transform=ax.transAxes, ha="center", va="center",
                            fontsize=8, color="#888")
                    ax.set_xticks([]); ax.set_yticks([])
                    continue

                try:
                    rgb, (west, south, east, north) = read_rgb(tif, ep_idx, bpe)
                except Exception as e:
                    ax.set_facecolor("#111")
                    ax.text(0.5, 0.5, f"Error:\n{e}",
                            transform=ax.transAxes, ha="center", va="center",
                            fontsize=7, color="red")
                    ax.set_xticks([]); ax.set_yticks([])
                    continue

                ax.imshow(rgb, extent=[west, east, south, north],
                          origin="upper", aspect="auto")

                # Draw routes for this epoch
                lbl    = f"T{ep}"
                rlist  = routes.get(lbl, [])
                for ri, r in enumerate(rlist):
                    coords = r.get("geometry", {}).get("coordinates", [])
                    if len(coords) < 2:
                        continue
                    xs = [c[0] for c in coords]
                    ys = [c[1] for c in coords]
                    ax.plot(xs, ys, color=ROUTE_COLORS[ri % len(ROUTE_COLORS)],
                            lw=1.8, alpha=0.85, zorder=4)
                    ax.scatter(xs[0], ys[0], c="#FF1744", s=25, zorder=6,
                               edgecolors="white", linewidths=0.4)

                # AOI bbox
                bbox = aoi["bbox"]
                rect = plt.Rectangle(
                    (bbox[0], bbox[1]), bbox[2]-bbox[0], bbox[3]-bbox[1],
                    linewidth=0.7, edgecolor="white", facecolor="none",
                    linestyle="--", zorder=3)
                ax.add_patch(rect)

                # Mark conflict segments on BOTH panels
                for c in aoi_conflicts:
                    seg_utm = c.get("segment_geom")
                    if seg_utm is None:
                        continue
                    # segment_geom is in UTM; skip plotting (no CRS transform here)
                    # Instead annotate with text
                ax.set_title(
                    f"{aoi_id}  T{ep}  "
                    f"({'safe' if ep == ep_T else 'CONFLICT — route invalidated'})",
                    fontsize=7,
                    color="black" if ep == ep_T else "#c0392b",
                    fontweight="bold" if ep == ep_T1 else "normal"
                )
                ax.set_xlim(west, east)
                ax.set_ylim(south, north)
                ax.set_xticks([]); ax.set_yticks([])

                # Annotate n_conflicts
                if ep == ep_T1:
                    ax.text(0.02, 0.97, f"{n_conflicts} conflict(s)",
                            transform=ax.transAxes, ha="left", va="top",
                            fontsize=8, color="#c0392b",
                            bbox=dict(boxstyle="round,pad=0.2", fc="white", alpha=0.8))

        leg_handles = [
            mlines.Line2D([], [], color=ROUTE_COLORS[i], lw=2, label=f"Route {i+1}")
            for i in range(3)
        ] + [
            mlines.Line2D([], [], marker="o", color="w",
                          markerfacecolor="#FF1744", markersize=6, label="Origin"),
            mpatches.Patch(facecolor="#c0392b", alpha=0.7, label="Route invalidated at T+1"),
        ]
        fig.legend(handles=leg_handles, fontsize=7, loc="lower right",
                   bbox_to_anchor=(0.99, 0.01), ncol=5)

        plt.tight_layout(rect=[0, 0.04, 1, 0.91])
        out = OUT_DIR / f"{event_id}_conflicts.png"
        fig.savefig(out, dpi=140, bbox_inches="tight")
        plt.close(fig)
        print(f"  {out.name}")

    print(f"\nDone -- {len(by_event)} conflict map PNGs in {OUT_DIR}")


if __name__ == "__main__":
    main()
