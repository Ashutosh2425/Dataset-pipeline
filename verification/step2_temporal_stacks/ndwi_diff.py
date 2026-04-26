"""
Step 2 Visual Verification — NDWI Epoch Maps + Change Map
----------------------------------------------------------
For the same 2 AOIs per event selected by rgb_strip.py, shows:
  Cols 0-3  : NDWI per epoch  (RdBu  blue=water  red=land)
  Col 4     : Δ NDWI (last epoch − T0)  positive=new water  negative=drained

What to look for:
  - Flood events: blue patch should APPEAR in during_1/2 and fade by during_3
  - Delta column: obvious blue blob where floodwater arrived
  - Flat grey across all epochs = suspicious (bad spectral data or arid site)

Output: verification/step2_temporal_stacks/ndwi_diffs/EVT{N}_ndwi.png

Run from repo root:
    python verification/step2_temporal_stacks/ndwi_diff.py
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
OUT_DIR     = Path(__file__).parent / "ndwi_diffs"

N_PER_EVENT = 2

EPOCH_COLORS = {
    "pre_event": "#4a90d9",
    "during_1":  "#e07b00",
    "during_2":  "#c0392b",
    "during_3":  "#7b241c",
}


def read_ndwi_epoch(tif_path, epoch_idx, bands_per_epoch):
    if bands_per_epoch[epoch_idx] < 4:
        return None   # missing B08 — caller handles this
    offset  = sum(bands_per_epoch[:epoch_idx])
    b_green = offset + 2   # B03
    b_nir   = offset + 4   # B08
    with rasterio.open(tif_path) as src:
        green = src.read(b_green).astype(np.float32)
        nir   = src.read(b_nir).astype(np.float32)
    denom = green + nir
    return np.where(denom > 0, (green - nir) / denom, 0.0)


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
        n_cols = 5   # 4 epochs + 1 delta
        fig, axes = plt.subplots(n_rows, n_cols,
                                 figsize=(n_cols * 3.2, n_rows * 3.0),
                                 squeeze=False)
        event_type = items[0][0]["event_type"]
        fig.suptitle(f"{event_id}  ({event_type})  —  NDWI per Epoch  +  Δ from T0\n"
                     "blue=water  red=land  ·  Δ col: blue=new water  red=drained",
                     fontsize=9, fontweight="bold")

        for row, (aoi, meta, labels, _) in enumerate(items):
            aoi_id = aoi["aoi_id"]
            tif    = STACKS_DIR / f"{aoi_id}_stack.tif"
            dates  = meta["dates"]
            bpe    = meta["bands_per_epoch"]
            n_ep   = len(dates)

            ndwi = [read_ndwi_epoch(tif, i, bpe) for i in range(n_ep)]

            # Find the last non-None NDWI for delta calculation
            last_ndwi = next((v for v in reversed(ndwi) if v is not None), None)

            for col in range(n_cols):
                ax = axes[row][col]
                ax.set_xticks([])
                ax.set_yticks([])

                if col < n_ep:
                    lbl   = labels[col] if col < len(labels) else "unknown"
                    color = EPOCH_COLORS.get(lbl, "#888888")
                    if ndwi[col] is None:
                        ax.text(0.5, 0.5, "no B08", ha="center", va="center",
                                fontsize=9, transform=ax.transAxes, color="gray")
                        ax.set_facecolor("#f0f0f0")
                    else:
                        ax.imshow(ndwi[col], cmap="RdBu", vmin=-0.5, vmax=0.5,
                                  interpolation="nearest")
                    ax.set_title(f"{lbl}\n{dates[col]}", fontsize=7,
                                 color=color, pad=3, fontweight="bold")
                    for spine in ax.spines.values():
                        spine.set_edgecolor(color)
                        spine.set_linewidth(2.5)

                elif col == 4:
                    if ndwi[0] is None or last_ndwi is None:
                        ax.text(0.5, 0.5, "N/A", ha="center", va="center",
                                fontsize=12, transform=ax.transAxes, color="gray")
                        ax.set_facecolor("#f0f0f0")
                        ax.set_title("Δ NDWI\nn/a", fontsize=7, color="#6a0dad",
                                     pad=3, fontweight="bold")
                        continue
                    delta = last_ndwi - ndwi[0]
                    ax.imshow(delta, cmap="coolwarm_r", vmin=-0.4, vmax=0.4,
                              interpolation="nearest")
                    ax.set_title(f"Δ NDWI\n(T{n_ep - 1} − T0)", fontsize=7,
                                 color="#6a0dad", pad=3, fontweight="bold")
                    for spine in ax.spines.values():
                        spine.set_edgecolor("#6a0dad")
                        spine.set_linewidth(2.5)

                else:
                    ax.set_visible(False)

            axes[row][0].text(-0.18, 0.5, aoi_id,
                              transform=axes[row][0].transAxes,
                              fontsize=8, va="center", ha="right")

        # Shared colourbars on the right
        fig.subplots_adjust(right=0.87, top=0.91)
        cb1_ax = fig.add_axes([0.89, 0.55, 0.016, 0.32])
        cb2_ax = fig.add_axes([0.89, 0.12, 0.016, 0.32])
        sm1 = plt.cm.ScalarMappable(cmap="RdBu",
                                     norm=plt.Normalize(-0.5, 0.5))
        sm2 = plt.cm.ScalarMappable(cmap="coolwarm_r",
                                     norm=plt.Normalize(-0.4, 0.4))
        sm1.set_array([])
        sm2.set_array([])
        fig.colorbar(sm1, cax=cb1_ax).set_label("NDWI", fontsize=8)
        fig.colorbar(sm2, cax=cb2_ax).set_label("Δ NDWI", fontsize=8)

        out = OUT_DIR / f"{event_id}_ndwi.png"
        fig.savefig(out, dpi=140, bbox_inches="tight")
        plt.close(fig)
        print(f"  {out.name}")

    print(f"\nDone — {len(by_event)} NDWI diff PNGs in {OUT_DIR}")


if __name__ == "__main__":
    main()
