"""
Step 2c Verification — False-Colour Change Composite
-----------------------------------------------------
For 2 AOIs per event, shows a 3-panel strip per AOI row:

  Col 0 : Pre-event RGB  (true colour)
  Col 1 : During-1 RGB   (true colour)
  Col 2 : Change composite  R=during_NIR  G=pre_NIR  B=pre_NIR
           RED/MAGENTA  = NIR dropped  -> flood / vegetation loss / damage
           GREEN        = NIR increased -> vegetation recovery / regrowth
           GREY         = no change

This is more diagnostic than NDWI diff alone because it works for
both flood (NIR suppressed by water) and structural damage
(NIR drops when vegetation/surface reflectance changes).

Output: verification/step2_temporal_stacks/change_composites/EVT{N}_change.png

Run from repo root:
    python verification/step2_temporal_stacks/step2c_change_composite.py
"""

import json
import numpy as np
import rasterio
from rasterio.warp import transform_bounds
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from pathlib import Path

BASE       = Path(__file__).resolve().parents[2]
STACKS_DIR = BASE / "data" / "stacks"
AOI_LIST   = json.load(open(BASE / "data" / "aoi_list.json"))
EPOCHS_MAP = json.load(open(BASE / "data" / "aoi_epochs.json"))
OUT_DIR    = Path(__file__).parent / "change_composites"
OUT_DIR.mkdir(exist_ok=True)

N_PER_EVENT = 2


def percentile_stretch(arr, lo=2, hi=98):
    valid = arr[arr > 0]
    if valid.size == 0:
        return np.zeros_like(arr, dtype=np.float32)
    vlo, vhi = np.percentile(valid, lo), np.percentile(valid, hi)
    if vhi <= vlo:
        return np.zeros_like(arr, dtype=np.float32)
    return np.clip((arr - vlo) / (vhi - vlo), 0, 1).astype(np.float32)


def read_epoch_bands(tif_path, epoch_idx, bpe):
    """Return (blue, green, red, nir) as float32 arrays for one epoch."""
    if bpe[epoch_idx] < 4:
        return None
    offset = sum(bpe[:epoch_idx])
    with rasterio.open(tif_path) as src:
        blue  = src.read(offset + 1).astype(np.float32)
        green = src.read(offset + 2).astype(np.float32)
        red   = src.read(offset + 3).astype(np.float32)
        nir   = src.read(offset + 4).astype(np.float32)
    return blue, green, red, nir


def make_rgb(blue, green, red):
    return np.stack([percentile_stretch(red),
                     percentile_stretch(green),
                     percentile_stretch(blue)], axis=-1)


def make_change_composite(pre_nir, during_nir):
    """
    False-colour change: R=during_NIR  G=pre_NIR  B=pre_NIR
    Stretch each independently so contrast is maximised.
    Red/magenta = NIR dropped (damage/flood)
    Green       = NIR increased (regrowth)
    """
    r = percentile_stretch(during_nir)
    g = percentile_stretch(pre_nir)
    b = percentile_stretch(pre_nir)
    return np.stack([r, g, b], axis=-1)


def epoch_index_for_label(aoi_id, label):
    for i, ep in enumerate(EPOCHS_MAP.get(aoi_id, [])):
        if ep["epoch_label"] == label:
            return i
    return None


def select_sample():
    by_event = {}
    for a in AOI_LIST:
        by_event.setdefault(a["event_id"], []).append(a)
    selected = []
    for eid in sorted(by_event):
        valid = []
        for a in by_event[eid]:
            tif  = STACKS_DIR / f"{a['aoi_id']}_stack.tif"
            meta = STACKS_DIR / f"{a['aoi_id']}_meta.json"
            if tif.exists() and meta.exists():
                m = json.load(open(meta))
                labels = [ep["epoch_label"] for ep in EPOCHS_MAP.get(a["aoi_id"], [])]
                if "pre_event" in labels and "during_1" in labels:
                    valid.append((a, m))
        selected.extend(valid[:N_PER_EVENT])
    return selected


def main():
    sample = select_sample()
    by_event = {}
    for item in sample:
        by_event.setdefault(item[0]["event_id"], []).append(item)

    for event_id, items in sorted(by_event.items()):
        event_type = items[0][0]["event_type"]
        n_rows = len(items)
        n_cols = 3  # pre RGB | during RGB | change composite

        fig, axes = plt.subplots(n_rows, n_cols,
                                 figsize=(n_cols * 4.0, n_rows * 4.0),
                                 squeeze=False)
        fig.suptitle(
            f"{event_id}  ({event_type})  —  False-Colour Change Composite\n"
            "Col 1: Pre RGB   Col 2: During-1 RGB   "
            "Col 3: Change  (Red=damage/flood  Green=regrowth  Grey=unchanged)",
            fontsize=9, fontweight="bold"
        )

        for row, (aoi, meta) in enumerate(items):
            aoi_id = aoi["aoi_id"]
            tif    = STACKS_DIR / f"{aoi_id}_stack.tif"
            bpe    = meta["bands_per_epoch"]
            dates  = meta["dates"]

            pre_idx    = epoch_index_for_label(aoi_id, "pre_event")
            during_idx = epoch_index_for_label(aoi_id, "during_1")

            if pre_idx is None or during_idx is None:
                for c in range(n_cols):
                    axes[row][c].set_visible(False)
                continue

            pre_bands    = read_epoch_bands(tif, pre_idx, bpe)
            during_bands = read_epoch_bands(tif, during_idx, bpe)

            if pre_bands is None or during_bands is None:
                for c in range(n_cols):
                    axes[row][c].set_visible(False)
                continue

            pre_rgb    = make_rgb(*pre_bands[:3])
            during_rgb = make_rgb(*during_bands[:3])
            change     = make_change_composite(pre_bands[3], during_bands[3])

            with rasterio.open(tif) as src:
                bounds = transform_bounds(src.crs, "EPSG:4326", *src.bounds)
            west, south, east, north = bounds
            extent = [west, east, south, north]

            imgs = [pre_rgb, during_rgb, change]
            titles = [
                f"Pre-event\n{dates[pre_idx]}",
                f"During-1\n{dates[during_idx]}",
                "Change composite\nRed=NIR drop  Green=NIR rise",
            ]

            for c, (img, title) in enumerate(zip(imgs, titles)):
                ax = axes[row][c]
                ax.imshow(img, extent=extent, origin="upper", aspect="auto")
                ax.set_title(title, fontsize=7.5)
                ax.set_xlim(west, east)
                ax.set_ylim(south, north)
                ax.set_xticks([])
                ax.set_yticks([])

            axes[row][0].text(-0.08, 0.5, aoi_id,
                              transform=axes[row][0].transAxes,
                              fontsize=8, va="center", ha="right")

        plt.tight_layout(rect=[0.05, 0.01, 1, 0.93])
        out = OUT_DIR / f"{event_id}_change.png"
        fig.savefig(out, dpi=140, bbox_inches="tight")
        plt.close(fig)
        print(f"  {out.name}")

    print(f"\nDone — {len(by_event)} change composite PNGs in {OUT_DIR}")


if __name__ == "__main__":
    main()
