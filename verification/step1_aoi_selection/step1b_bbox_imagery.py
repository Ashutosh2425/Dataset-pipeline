"""
Step 1b Verification — AOI Bbox on Pre-Event Imagery
-----------------------------------------------------
For each event, shows a grid of AOI thumbnails (pre-event RGB from the
actual downloaded stack) with the AOI bbox drawn as a red dashed rectangle.

What to check:
  - Does the bbox sit over the correct geographic location?
  - Is the imagery showing the right disaster region (urban/coastal/forest)?
  - Does the red box tightly match the visible terrain boundary?

Any bbox sitting over wrong terrain (e.g. ocean instead of city) = Step 1 error.

Output: verification/step1_aoi_selection/step1b_imagery/EVT{N}_bbox.png

Run from repo root:
    python verification/step1_aoi_selection/step1b_bbox_imagery.py
"""

import json
import numpy as np
import rasterio
from rasterio.warp import transform_bounds
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from pathlib import Path

BASE       = Path(__file__).resolve().parents[2]
STACKS_DIR = BASE / "data" / "stacks"
AOI_LIST   = json.load(open(BASE / "data" / "aoi_list.json"))
EPOCHS_MAP = json.load(open(BASE / "data" / "aoi_epochs.json"))
OUT_DIR    = Path(__file__).parent / "step1b_imagery"
OUT_DIR.mkdir(exist_ok=True)

N_PER_EVENT = 6   # show up to 6 AOIs per event in a 2×3 grid


def percentile_stretch(arr, lo=2, hi=98):
    valid = arr[arr > 0]
    if valid.size == 0:
        return np.zeros_like(arr, dtype=np.float32)
    vlo, vhi = np.percentile(valid, lo), np.percentile(valid, hi)
    if vhi <= vlo:
        return np.zeros_like(arr, dtype=np.float32)
    return np.clip((arr - vlo) / (vhi - vlo), 0, 1).astype(np.float32)


def read_pre_event_rgb(tif_path, bpe):
    """Read RGB from the pre-event epoch (epoch index 0)."""
    with rasterio.open(tif_path) as src:
        blue  = src.read(1).astype(np.float32)
        green = src.read(2).astype(np.float32)
        red   = src.read(3).astype(np.float32)
        bounds_wgs84 = transform_bounds(src.crs, "EPSG:4326", *src.bounds)
    rgb = np.stack([percentile_stretch(red),
                    percentile_stretch(green),
                    percentile_stretch(blue)], axis=-1)
    return rgb, bounds_wgs84   # (west, south, east, north)


def pre_event_epoch_offset(aoi_id):
    """Return band offset for the pre_event epoch (should be 0)."""
    for i, ep in enumerate(EPOCHS_MAP.get(aoi_id, [])):
        if ep["epoch_label"] == "pre_event":
            return i
    return 0   # default to first epoch


def select_sample():
    by_event = {}
    for a in AOI_LIST:
        by_event.setdefault(a["event_id"], []).append(a)

    selected = {}
    for eid, aois in by_event.items():
        valid = []
        for a in aois:
            tif  = STACKS_DIR / f"{a['aoi_id']}_stack.tif"
            meta = STACKS_DIR / f"{a['aoi_id']}_meta.json"
            if tif.exists() and meta.exists():
                valid.append(a)
        selected[eid] = valid[:N_PER_EVENT]
    return selected


def main():
    sample = select_sample()

    for event_id, aois in sorted(sample.items()):
        if not aois:
            continue

        event_type = aois[0]["event_type"]
        n = len(aois)
        n_cols = min(3, n)
        n_rows = (n + n_cols - 1) // n_cols

        fig, axes = plt.subplots(n_rows, n_cols,
                                 figsize=(n_cols * 4.2, n_rows * 4.2),
                                 squeeze=False)
        fig.suptitle(
            f"{event_id}  ({event_type})  —  AOI Bbox on Pre-Event Imagery\n"
            "Red dashed box = AOI bbox from aoi_list.json\n"
            "Verify: box should sit over the correct disaster terrain",
            fontsize=9, fontweight="bold"
        )

        for idx, aoi in enumerate(aois):
            r, c    = divmod(idx, n_cols)
            ax      = axes[r][c]
            aoi_id  = aoi["aoi_id"]
            tif     = STACKS_DIR / f"{aoi_id}_stack.tif"
            meta    = json.load(open(STACKS_DIR / f"{aoi_id}_meta.json"))
            bpe     = meta["bands_per_epoch"]

            try:
                rgb, (west, south, east, north) = read_pre_event_rgb(tif, bpe)
            except Exception as e:
                ax.text(0.5, 0.5, f"read error\n{e}", ha="center", va="center",
                        fontsize=7, transform=ax.transAxes, color="red")
                ax.set_title(aoi_id, fontsize=7)
                continue

            # Show imagery in its actual geographic extent
            ax.imshow(rgb, extent=[west, east, south, north],
                      origin="upper", aspect="auto")

            # Draw the AOI bbox as a red dashed rectangle
            bw, bs, be, bn = aoi["bbox"]
            rect = mpatches.Rectangle(
                (bw, bs), be - bw, bn - bs,
                linewidth=2, edgecolor="red", facecolor="none",
                linestyle="--", zorder=5
            )
            ax.add_patch(rect)

            # Mark centroid
            cx = (bw + be) / 2
            cy = (bs + bn) / 2
            ax.plot(cx, cy, "r+", markersize=10, markeredgewidth=1.5, zorder=6)

            # Show TIF bounds vs bbox delta
            tif_w = east - west
            tif_h = north - south
            bbox_w = be - bw
            bbox_h = bn - bs
            dw = abs(tif_w - bbox_w) * 111000
            dh = abs(tif_h - bbox_h) * 111000

            pre_date = meta["dates"][0] if meta["dates"] else "?"
            ax.set_title(f"{aoi_id}\npre: {pre_date}   Δ bbox/tif: {dw:.0f}m × {dh:.0f}m",
                         fontsize=6.5)
            ax.set_xlim(west - 0.002, east + 0.002)
            ax.set_ylim(south - 0.002, north + 0.002)
            ax.set_xticks([])
            ax.set_yticks([])

        # Hide unused cells
        for idx in range(len(aois), n_rows * n_cols):
            r, c = divmod(idx, n_cols)
            axes[r][c].set_visible(False)

        # Legend
        bbox_patch = mpatches.Patch(facecolor="none", edgecolor="red",
                                    linestyle="--", linewidth=1.5,
                                    label="AOI bbox (aoi_list.json)")
        fig.legend(handles=[bbox_patch], fontsize=8,
                   loc="lower right", bbox_to_anchor=(0.99, 0.01))

        plt.tight_layout(rect=[0, 0.02, 1, 0.93])
        out = OUT_DIR / f"{event_id}_bbox.png"
        fig.savefig(out, dpi=140, bbox_inches="tight")
        plt.close(fig)
        print(f"  {out.name}")

    print(f"\nDone — {len(sample)} bbox imagery PNGs in {OUT_DIR}")


if __name__ == "__main__":
    main()
