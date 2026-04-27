"""
Step 2 Visual Verification — RGB Epoch Strips
----------------------------------------------
Renders 2 AOIs per event as per-epoch true-colour thumbnails
(R=B04  G=B03  B=B02). Epochs are border-colored by label:
  blue = pre_event  orange = during_1  red = during_2  dark-red = during_3

Output: verification/step2_temporal_stacks/rgb_strips/EVT{N}_rgb.png
        (12 PNGs, one per event)

Run from repo root:
    python verification/step2_temporal_stacks/rgb_strip.py
"""

import json
import numpy as np
import rasterio
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from pathlib import Path

BASE        = Path(__file__).resolve().parents[2]
STACKS_DIR  = BASE / "data" / "stacks"
AOI_LIST    = json.load(open(BASE / "data" / "aoi_list.json"))
EPOCHS_MAP  = json.load(open(BASE / "data" / "aoi_epochs.json"))
OUT_DIR     = Path(__file__).parent / "rgb_strips"

N_PER_EVENT = 2

EPOCH_COLORS = {
    "pre_event": "#4a90d9",
    "during_1":  "#e07b00",
    "during_2":  "#c0392b",
    "during_3":  "#7b241c",
}


def percentile_stretch(arr, lo=2, hi=98):
    valid = arr[arr > 0]
    if valid.size == 0:
        return np.zeros_like(arr, dtype=np.float32)
    vlo, vhi = np.percentile(valid, lo), np.percentile(valid, hi)
    if vhi <= vlo:
        return np.zeros_like(arr, dtype=np.float32)
    return np.clip((arr - vlo) / (vhi - vlo), 0, 1).astype(np.float32)


def read_rgb_epoch(tif_path, epoch_idx, bands_per_epoch):
    offset  = sum(bands_per_epoch[:epoch_idx])
    b_blue  = offset + 1   # B02
    b_green = offset + 2   # B03
    b_red   = offset + 3   # B04
    with rasterio.open(tif_path) as src:
        blue  = src.read(b_blue).astype(np.float32)
        green = src.read(b_green).astype(np.float32)
        red   = src.read(b_red).astype(np.float32)
    return np.stack([percentile_stretch(red),
                     percentile_stretch(green),
                     percentile_stretch(blue)], axis=-1)


def epoch_label(aoi_id, date):
    for ep in EPOCHS_MAP.get(aoi_id, []):
        if ep["date"] == date:
            return ep["epoch_label"]
    return "unknown"


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
            if not (tif.exists() and meta.exists()):
                continue
            m      = json.load(open(meta))
            labels = [epoch_label(a["aoi_id"], d) for d in m["dates"]]
            has_pre = "pre_event" in labels
            valid.append((a, m, labels, has_pre))
        valid.sort(key=lambda x: (not x[3], x[0]["aoi_id"]))
        selected.extend(valid[:N_PER_EVENT])
    return selected


def main():
    OUT_DIR.mkdir(exist_ok=True)
    sample = select_sample()

    by_event = {}
    for item in sample:
        by_event.setdefault(item[0]["event_id"], []).append(item)

    for event_id, items in sorted(by_event.items()):
        n_rows = len(items)
        n_cols = 4
        fig, axes = plt.subplots(n_rows, n_cols,
                                 figsize=(n_cols * 3.2, n_rows * 3.0),
                                 squeeze=False)
        event_type = items[0][0]["event_type"]
        fig.suptitle(f"{event_id}  ({event_type})  —  RGB Epoch Strip\n"
                     "R=B04  G=B03  B=B02  ·  border colour = epoch label",
                     fontsize=9, fontweight="bold")

        for row, (aoi, meta, labels, _) in enumerate(items):
            aoi_id = aoi["aoi_id"]
            tif    = STACKS_DIR / f"{aoi_id}_stack.tif"
            dates  = meta["dates"]
            bpe    = meta["bands_per_epoch"]

            for col in range(n_cols):
                ax = axes[row][col]
                if col < len(dates):
                    rgb   = read_rgb_epoch(tif, col, bpe)
                    lbl   = labels[col] if col < len(labels) else "unknown"
                    color = EPOCH_COLORS.get(lbl, "#888888")
                    ax.imshow(rgb)
                    ax.set_title(f"{lbl}\n{dates[col]}", fontsize=7,
                                 color=color, pad=3, fontweight="bold")
                    for spine in ax.spines.values():
                        spine.set_edgecolor(color)
                        spine.set_linewidth(2.5)
                else:
                    ax.set_visible(False)
                    continue
                ax.set_xticks([])
                ax.set_yticks([])

            # Row label
            axes[row][0].text(-0.18, 0.5, aoi_id,
                              transform=axes[row][0].transAxes,
                              fontsize=8, va="center", ha="right",
                              rotation=0)

        plt.tight_layout(rect=[0.08, 0, 1, 0.94])
        out = OUT_DIR / f"{event_id}_rgb.png"
        fig.savefig(out, dpi=140, bbox_inches="tight")
        plt.close(fig)
        print(f"  {out.name}")

    print(f"\nDone — {len(by_event)} RGB strip PNGs in {OUT_DIR}")


if __name__ == "__main__":
    main()
