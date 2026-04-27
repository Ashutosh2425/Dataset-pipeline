"""
Step 2 Visual Verification — Temporal Water Fraction Profile
-------------------------------------------------------------
Computes the fraction of pixels with NDWI > 0.2 for EVERY AOI × epoch
and plots it as a time-series, grouped by event.

What to look for:
  - Flood events  : water fraction should SPIKE at during_1 or during_2,
                    then drop back toward during_3. Flat = bad data.
  - Non-flood     : low flat profile is expected (quake, wind, fire).
  - Very high pre : coastal / riverine site, normal.
  - AOIs that never exceed 0.02 for a flood event = suspect.

Reads TIFs at 1/4 resolution for speed (~2-3 min for 594 AOIs).

Output: verification/step2_temporal_stacks/water_fraction_profile.png

Run from repo root:
    python verification/step2_temporal_stacks/water_fraction_profile.py
"""

import json
import numpy as np
import rasterio
from rasterio.enums import Resampling
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from pathlib import Path
from tqdm import tqdm

BASE        = Path(__file__).resolve().parents[2]
STACKS_DIR  = BASE / "data" / "stacks"
AOI_LIST    = json.load(open(BASE / "data" / "aoi_list.json"))
EPOCHS_MAP  = json.load(open(BASE / "data" / "aoi_epochs.json"))
OUT_FILE    = Path(__file__).parent / "water_fraction_profile.png"

NDWI_THRESH = 0.2
DOWNSAMPLE  = 4

LABEL_ORDER = ["pre_event", "during_1", "during_2", "during_3"]
X_LABELS    = ["pre\nevent", "during\n1", "during\n2", "during\n3"]

EVENT_COLORS = {
    "EVT001": "#1f77b4", "EVT002": "#ff7f0e", "EVT003": "#2ca02c",
    "EVT004": "#d62728", "EVT005": "#9467bd", "EVT006": "#8c564b",
    "EVT007": "#e377c2", "EVT008": "#7f7f7f", "EVT009": "#bcbd22",
    "EVT010": "#17becf", "EVT011": "#aec7e8", "EVT012": "#ffbb78",
}


def epoch_label(aoi_id, date):
    for ep in EPOCHS_MAP.get(aoi_id, []):
        if ep["date"] == date:
            return ep["epoch_label"]
    return "unknown"


def water_fractions(aoi_id, meta):
    """Return {epoch_label: water_fraction} for one AOI."""
    tif   = STACKS_DIR / f"{aoi_id}_stack.tif"
    bpe   = meta["bands_per_epoch"]
    dates = meta["dates"]
    result = {}

    with rasterio.open(tif) as src:
        H, W  = src.height, src.width
        out_h = max(1, H // DOWNSAMPLE)
        out_w = max(1, W // DOWNSAMPLE)

        for i, date in enumerate(dates):
            lbl    = epoch_label(aoi_id, date)
            offset = sum(bpe[:i])
            if bpe[i] < 4:
                # Missing B08 (NIR) — can't compute NDWI, skip epoch
                result[lbl] = None
                continue
            green  = src.read(offset + 2,
                              out_shape=(1, out_h, out_w),
                              resampling=Resampling.average)[0].astype(np.float32)
            nir    = src.read(offset + 4,
                              out_shape=(1, out_h, out_w),
                              resampling=Resampling.average)[0].astype(np.float32)
            denom  = green + nir
            ndwi   = np.where(denom > 0, (green - nir) / denom, 0.0)
            result[lbl] = float((ndwi > NDWI_THRESH).mean())

    return result


def main():
    # Build aoi_id → (aoi_dict, meta) for existing stacks
    aoi_meta = {}
    for a in AOI_LIST:
        meta_p = STACKS_DIR / f"{a['aoi_id']}_meta.json"
        tif_p  = STACKS_DIR / f"{a['aoi_id']}_stack.tif"
        if meta_p.exists() and tif_p.exists():
            aoi_meta[a["aoi_id"]] = (a, json.load(open(meta_p)))

    print(f"Computing water fractions for {len(aoi_meta)} AOIs "
          f"(1/{DOWNSAMPLE} resolution)...")

    # event_id → list of per-label fraction arrays
    data = {}
    for aoi_id, (aoi, meta) in tqdm(sorted(aoi_meta.items())):
        fracs = water_fractions(aoi_id, meta)
        eid   = aoi["event_id"]
        data.setdefault(eid, []).append(fracs)

    # Plot
    events = sorted(data.keys())
    n_cols = 4
    n_rows = (len(events) + n_cols - 1) // n_cols
    x      = list(range(len(LABEL_ORDER)))

    fig, axes = plt.subplots(n_rows, n_cols,
                             figsize=(n_cols * 3.6, n_rows * 3.2),
                             squeeze=False)
    fig.suptitle(
        f"Step 2 — Temporal Water Fraction  (NDWI > {NDWI_THRESH})\n"
        "Gray lines = individual AOIs  ·  Bold = event mean",
        fontsize=11, fontweight="bold"
    )

    for idx, event_id in enumerate(events):
        row, col = divmod(idx, n_cols)
        ax    = axes[row][col]
        color = EVENT_COLORS.get(event_id, "steelblue")

        etype    = next((a["event_type"] for a in AOI_LIST
                         if a["event_id"] == event_id), "")
        all_rows = data[event_id]

        matrix = []
        for frac_map in all_rows:
            y = [frac_map.get(lbl) for lbl in LABEL_ORDER]
            valid = [(xi, yi) for xi, yi in zip(x, y) if yi is not None]
            if len(valid) >= 2:
                xv, yv = zip(*valid)
                ax.plot(xv, yv, color="gray", alpha=0.2, lw=0.7, zorder=1)
            matrix.append([v if v is not None else np.nan for v in y])

        if matrix:
            arr  = np.array(matrix)
            mean = np.nanmean(arr, axis=0)
            ax.plot(x, mean, color=color, lw=2.5, marker="o",
                    markersize=5, zorder=3, label="mean")

        ax.set_title(f"{event_id}  ({etype})  n={len(all_rows)}", fontsize=8)
        ax.set_xticks(x)
        ax.set_xticklabels(X_LABELS, fontsize=7)
        ax.set_ylim(0, None)
        ax.set_ylabel("Water fraction", fontsize=7)
        ax.axvline(x=0.5, color="firebrick", lw=0.9, ls="--",
                   alpha=0.55, label="event onset →")
        ax.legend(fontsize=6, loc="upper right")
        ax.grid(axis="y", alpha=0.3, lw=0.5)

    # Hide unused subplots
    for idx in range(len(events), n_rows * n_cols):
        r, c = divmod(idx, n_cols)
        axes[r][c].set_visible(False)

    plt.tight_layout(rect=[0, 0, 1, 0.94])
    fig.savefig(OUT_FILE, dpi=140, bbox_inches="tight")
    plt.close(fig)
    print(f"\nSaved: {OUT_FILE}")


if __name__ == "__main__":
    main()
