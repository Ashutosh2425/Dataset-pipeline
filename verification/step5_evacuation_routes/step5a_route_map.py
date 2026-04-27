"""
Step 5a Verification — Evacuation Route Map on RGB Imagery
----------------------------------------------------------
For 2 AOIs per event (those with the most routes), overlays evacuation
route LineStrings on the RGB Sentinel-2 thumbnail for each post-event epoch.

Route colours:
  blue   = Route 1 (primary / shortest-safe)
  orange = Route 2
  green  = Route 3
  red dot  = population centroid (origin)
  star     = shelter (destination)

Dashed white box = AOI bounding box.

Expected: routes follow real road corridors and avoid flooded/destroyed areas.
Cross-check against the damage overlay (step3a) and flood overlay (step3b).

Output: verification/step5_evacuation_routes/step5a_maps/EVT{N}_routes.png

Run from repo root:
    python verification/step5_evacuation_routes/step5a_route_map.py
"""

import json
import numpy as np
import rasterio
from rasterio.warp import transform_bounds
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import matplotlib.lines as mlines
from pathlib import Path

BASE       = Path(__file__).resolve().parents[2]
STACKS_DIR = BASE / "data" / "stacks"
ANN_DIR    = BASE / "data" / "annotations"
AOI_LIST   = json.load(open(BASE / "data" / "aoi_list.json"))
EPOCHS_MAP = json.load(open(BASE / "data" / "aoi_epochs.json"))
OUT_DIR    = Path(__file__).parent / "step5a_maps"
OUT_DIR.mkdir(exist_ok=True)

N_PER_EVENT  = 2
ROUTE_COLORS = ["#2196F3", "#FF9800", "#4CAF50", "#9C27B0", "#FF5722"]
EPOCH_LABELS = ["T2", "T3", "T4"]


def percentile_stretch(arr, lo=2, hi=98):
    valid = arr[arr > 0]
    if valid.size == 0:
        return np.zeros_like(arr, dtype=np.float32)
    vlo, vhi = np.percentile(valid, lo), np.percentile(valid, hi)
    if vhi <= vlo:
        return np.zeros_like(arr, dtype=np.float32)
    return np.clip((arr - vlo) / (vhi - vlo), 0, 1).astype(np.float32)


def read_rgb(tif_path, epoch_idx, bpe):
    """Read RGB bands for a given epoch index (0-based)."""
    offset = sum(bpe[:epoch_idx])
    with rasterio.open(tif_path) as src:
        b = src.read(offset + 1).astype(np.float32)
        g = src.read(offset + 2).astype(np.float32)
        r = src.read(offset + 3).astype(np.float32)
        bounds_wgs84 = transform_bounds(src.crs, "EPSG:4326", *src.bounds)
    rgb = np.stack([percentile_stretch(r), percentile_stretch(g),
                    percentile_stretch(b)], axis=-1)
    return rgb, bounds_wgs84  # (west, south, east, north)


def epoch_idx_for_label(aoi_id, label):
    """Return 0-based epoch index for T2/T3/T4 label."""
    n = int(label[1])   # T2->2, T3->3, T4->4
    return n - 1        # 0-based index into bpe


def select_sample():
    """2 AOIs per event that have routes and a stack TIF."""
    by_event = {}
    for a in AOI_LIST:
        by_event.setdefault(a["event_id"], []).append(a)

    selected = []
    for eid in sorted(by_event):
        candidates = []
        for a in by_event[eid]:
            tif  = STACKS_DIR / f"{a['aoi_id']}_stack.tif"
            meta = STACKS_DIR / f"{a['aoi_id']}_meta.json"
            rjp  = ANN_DIR / a["aoi_id"] / "evacuation_routes.json"
            if not (tif.exists() and meta.exists() and rjp.exists()):
                continue
            routes = json.load(open(rjp))
            n_routes = sum(len(v) for v in routes.values())
            if n_routes > 0:
                candidates.append((a, json.load(open(meta)), routes, n_routes))
        candidates.sort(key=lambda x: -x[3])
        selected.extend(candidates[:N_PER_EVENT])
    return selected


def draw_routes_on_ax(ax, routes, epoch_label):
    """Draw route lines, centroids, shelters for one epoch."""
    epoch_routes = routes.get(epoch_label, [])
    if not epoch_routes:
        ax.text(0.5, 0.5, f"No routes\nfor {epoch_label}",
                transform=ax.transAxes, ha="center", va="center",
                fontsize=8, color="white",
                bbox=dict(boxstyle="round,pad=0.3", fc="#333", alpha=0.8))
        return

    plotted_shelters = set()
    for i, r in enumerate(epoch_routes):
        coords = r.get("geometry", {}).get("coordinates", [])
        if len(coords) < 2:
            continue
        col = ROUTE_COLORS[i % len(ROUTE_COLORS)]
        xs  = [c[0] for c in coords]
        ys  = [c[1] for c in coords]
        ax.plot(xs, ys, color=col, lw=2.0, alpha=0.9, zorder=4,
                solid_capstyle="round")

        # Origin dot (first coord)
        ax.scatter(xs[0], ys[0], c="#FF1744", s=35, zorder=6,
                   edgecolors="white", linewidths=0.5)
        # Shelter star (last coord)
        sname = r.get("shelter_name", "?")
        if sname not in plotted_shelters:
            ax.scatter(xs[-1], ys[-1], marker="*", c="#FFD600", s=80,
                       zorder=6, edgecolors="white", linewidths=0.3)
            plotted_shelters.add(sname)

        # Route length annotation
        length_m = r.get("length_m", 0)
        rss      = r.get("rss", 0)
        ax.annotate(f"R{i+1}: {int(length_m)}m  rss={rss}",
                    xy=(xs[len(xs)//2], ys[len(ys)//2]),
                    fontsize=5.5, color=col, zorder=7,
                    bbox=dict(boxstyle="round,pad=0.1", fc="black", alpha=0.5))


def main():
    sample = select_sample()
    by_event = {}
    for item in sample:
        by_event.setdefault(item[0]["event_id"], []).append(item)

    for event_id, items in sorted(by_event.items()):
        event_type = items[0][0]["event_type"]
        n_rows = len(items)
        n_cols = len(EPOCH_LABELS)

        fig, axes = plt.subplots(n_rows, n_cols,
                                 figsize=(n_cols * 4.5, n_rows * 4.2),
                                 squeeze=False)
        fig.suptitle(
            f"{event_id}  ({event_type})  —  Step 5: Evacuation Routes on RGB\n"
            "Blue/Orange/Green = routes 1-3   Red dot = origin   "
            "Yellow star = shelter",
            fontsize=9, fontweight="bold"
        )

        for row, (aoi, meta, routes, _) in enumerate(items):
            aoi_id = aoi["aoi_id"]
            tif    = STACKS_DIR / f"{aoi_id}_stack.tif"
            bpe    = meta["bands_per_epoch"]
            n_ep   = meta["n_epochs"]

            for col, lbl in enumerate(EPOCH_LABELS):
                ax = axes[row][col]
                ep_idx = epoch_idx_for_label(aoi_id, lbl)

                if ep_idx >= n_ep:
                    ax.set_facecolor("#111")
                    ax.text(0.5, 0.5, f"{lbl} not available",
                            transform=ax.transAxes, ha="center", va="center",
                            fontsize=8, color="#888")
                    ax.set_xticks([]); ax.set_yticks([])
                    continue

                try:
                    rgb, (west, south, east, north) = read_rgb(tif, ep_idx, bpe)
                except Exception as e:
                    ax.set_facecolor("#111")
                    ax.text(0.5, 0.5, f"Read error:\n{e}",
                            transform=ax.transAxes, ha="center", va="center",
                            fontsize=7, color="red")
                    ax.set_xticks([]); ax.set_yticks([])
                    continue

                ax.imshow(rgb, extent=[west, east, south, north],
                          origin="upper", aspect="auto")

                # AOI bbox dashed outline
                bbox = aoi["bbox"]
                rect = plt.Rectangle(
                    (bbox[0], bbox[1]), bbox[2]-bbox[0], bbox[3]-bbox[1],
                    linewidth=0.8, edgecolor="white", facecolor="none",
                    linestyle="--", zorder=3)
                ax.add_patch(rect)

                draw_routes_on_ax(ax, routes, lbl)

                n_ep_routes = len(routes.get(lbl, []))
                ax.set_title(f"{aoi_id}  {lbl}  ({n_ep_routes} routes)",
                             fontsize=7)
                ax.set_xlim(west, east)
                ax.set_ylim(south, north)
                ax.set_xticks([]); ax.set_yticks([])

        # Legend
        leg_handles = [
            mlines.Line2D([], [], color=ROUTE_COLORS[i], lw=2,
                          label=f"Route {i+1}")
            for i in range(3)
        ] + [
            mlines.Line2D([], [], marker="o", color="w",
                          markerfacecolor="#FF1744", markersize=6,
                          label="Origin (centroid)"),
            mlines.Line2D([], [], marker="*", color="w",
                          markerfacecolor="#FFD600", markersize=9,
                          label="Shelter"),
        ]
        fig.legend(handles=leg_handles, fontsize=7, loc="lower right",
                   bbox_to_anchor=(0.99, 0.01), ncol=5)

        plt.tight_layout(rect=[0, 0.04, 1, 0.93])
        out = OUT_DIR / f"{event_id}_routes.png"
        fig.savefig(out, dpi=140, bbox_inches="tight")
        plt.close(fig)
        print(f"  {out.name}")

    print(f"\nDone — {len(by_event)} route map PNGs in {OUT_DIR}")


if __name__ == "__main__":
    main()
