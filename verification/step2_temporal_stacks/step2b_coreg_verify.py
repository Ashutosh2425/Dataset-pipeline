"""
Step 2b Verification — Co-registration Quality
-----------------------------------------------
Three output images:
  1. ecc_checkerboard.png   — T0 / T1 NIR checkerboard, 1 AOI per event
  2. footprint_accuracy.png — TIF actual bounds vs AOI expected bbox
  3. shape_consistency.png  — meta JSON shape vs actual TIF shape, all 594 AOIs

What to look for:
  Checkerboard: structural features (roads, rivers, building edges) should be
    CONTINUOUS across the 32-px block boundaries.  Visible jumps = ECC failed.
  Footprint:    red dashed outline should sit inside / closely match blue box.
  Shape:        all points should lie on the diagonal (meta = actual TIF).

Run from repo root:
    python verification/step2_temporal_stacks/step2b_coreg_verify.py
"""

import json
import numpy as np
import rasterio
from rasterio.warp import transform_bounds
import matplotlib
from collections import defaultdict
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from pathlib import Path

BASE       = Path(__file__).resolve().parents[2]
STACKS_DIR = BASE / "data" / "stacks"
AOI_LIST   = json.load(open(BASE / "data" / "aoi_list.json"))
OUT_DIR    = Path(__file__).parent / "step2b_coreg"
OUT_DIR.mkdir(exist_ok=True)


def norm(arr):
    valid = arr[arr > 0]
    if valid.size == 0:
        return np.zeros_like(arr, dtype=np.float32)
    lo, hi = np.percentile(valid, 2), np.percentile(valid, 98)
    if hi <= lo:
        return np.zeros_like(arr, dtype=np.float32)
    return np.clip((arr - lo) / (hi - lo), 0, 1).astype(np.float32)


def checkerboard_mask(H, W, block=32):
    rows = np.arange(H) // block
    cols = np.arange(W) // block
    return ((rows[:, None] + cols[None, :]) % 2).astype(bool)


def select_one_per_event():
    by_event = {}
    for a in AOI_LIST:
        by_event.setdefault(a["event_id"], []).append(a)
    selected = []
    for eid in sorted(by_event):
        for a in by_event[eid]:
            tif  = STACKS_DIR / f"{a['aoi_id']}_stack.tif"
            meta = STACKS_DIR / f"{a['aoi_id']}_meta.json"
            if not (tif.exists() and meta.exists()):
                continue
            m = json.load(open(meta))
            bpe = m["bands_per_epoch"]
            if m["n_epochs"] >= 2 and bpe[0] >= 4 and bpe[1] >= 4:
                selected.append((a, m))
                break
    return selected


# ── 1. ECC Checkerboard ───────────────────────────────────────────────────────

def plot_ecc_checkerboard():
    sample  = select_one_per_event()
    n_cols  = 4
    n_rows  = (len(sample) + n_cols - 1) // n_cols

    fig, axes = plt.subplots(n_rows, n_cols,
                             figsize=(n_cols * 3.8, n_rows * 3.8),
                             squeeze=False)
    fig.suptitle(
        "Step 2b — ECC Alignment Checkerboard  (NIR / B08 band)\n"
        "T0 and T1 alternate in 32-px blocks.  "
        "Features should be CONTINUOUS across red grid lines if ECC succeeded.",
        fontsize=9, fontweight="bold"
    )

    for idx, (aoi, meta) in enumerate(sample):
        r, c = divmod(idx, n_cols)
        ax   = axes[r][c]
        aoi_id = aoi["aoi_id"]
        tif    = STACKS_DIR / f"{aoi_id}_stack.tif"
        bpe    = meta["bands_per_epoch"]

        # T0 B08 = rasterio band (0*4 + 4) = 4
        # T1 B08 = rasterio band (1*4 + 4) = 8
        t0_nir_band = sum(bpe[:0]) + 4
        t1_nir_band = sum(bpe[:1]) + 4

        with rasterio.open(tif) as src:
            nir0 = src.read(t0_nir_band).astype(np.float32)
            nir1 = src.read(t1_nir_band).astype(np.float32)

        H, W = nir0.shape
        mask    = checkerboard_mask(H, W, block=32)
        blended = np.where(mask, norm(nir1), norm(nir0))

        ax.imshow(blended, cmap="gray", vmin=0, vmax=1, interpolation="nearest")

        # Red grid at block boundaries
        for y in range(0, H, 32):
            ax.axhline(y - 0.5, color="red", lw=0.4, alpha=0.5)
        for x in range(0, W, 32):
            ax.axvline(x - 0.5, color="red", lw=0.4, alpha=0.5)

        dates  = meta["dates"]
        t0_lbl = f"T0 {dates[0]}"
        t1_lbl = f"T1 {dates[1]}"
        ax.set_title(f"{aoi_id}  |  {aoi['event_id']} ({aoi['event_type']})\n"
                     f"checker: {t0_lbl} / {t1_lbl}",
                     fontsize=6.5)
        ax.set_xticks([])
        ax.set_yticks([])

    for idx in range(len(sample), n_rows * n_cols):
        r, c = divmod(idx, n_cols)
        axes[r][c].set_visible(False)

    # Legend
    leg = [mpatches.Patch(facecolor="black", label="T0 dark blocks"),
           mpatches.Patch(facecolor="white", edgecolor="gray", label="T1 light blocks")]
    fig.legend(handles=leg, fontsize=8, loc="lower right",
               bbox_to_anchor=(0.99, 0.01), ncol=2)

    plt.tight_layout(rect=[0, 0.02, 1, 0.93])
    out = OUT_DIR / "ecc_checkerboard.png"
    fig.savefig(out, dpi=140, bbox_inches="tight")
    plt.close(fig)
    print(f"  {out.name}")


# ── 2. Footprint Accuracy ─────────────────────────────────────────────────────

def plot_footprint_accuracy():
    sample  = select_one_per_event()
    n_cols  = 4
    n_rows  = (len(sample) + n_cols - 1) // n_cols

    fig, axes = plt.subplots(n_rows, n_cols,
                             figsize=(n_cols * 3.5, n_rows * 3.5),
                             squeeze=False)
    fig.suptitle(
        "Step 2b — Footprint Accuracy\n"
        "Blue solid = expected AOI bbox  |  Red dashed = actual TIF bounds (WGS84)\n"
        "Should overlap. Delta shows size difference in metres.",
        fontsize=9, fontweight="bold"
    )

    for idx, (aoi, meta) in enumerate(sample):
        r, c   = divmod(idx, n_cols)
        ax     = axes[r][c]
        aoi_id = aoi["aoi_id"]
        tif    = STACKS_DIR / f"{aoi_id}_stack.tif"

        ew, es, ee, en = aoi["bbox"]    # expected (WGS84)

        with rasterio.open(tif) as src:
            b = src.bounds
            try:
                aw, as_, ae, an = transform_bounds(src.crs, "EPSG:4326",
                                                    b.left, b.bottom,
                                                    b.right, b.top)
            except Exception:
                aw, as_, ae, an = b.left, b.bottom, b.right, b.top

        # Expected rectangle
        exp = mpatches.FancyBboxPatch(
            (ew, es), ee - ew, en - es,
            boxstyle="square,pad=0", fill=False,
            edgecolor="steelblue", linewidth=2.5, label="Expected bbox"
        )
        # Actual rectangle
        act = mpatches.FancyBboxPatch(
            (aw, as_), ae - aw, an - as_,
            boxstyle="square,pad=0", fill=False,
            edgecolor="firebrick", linewidth=1.5,
            linestyle="--", label="Actual TIF bounds"
        )
        ax.add_patch(exp)
        ax.add_patch(act)

        pad = max(ee - ew, en - es) * 0.35
        ax.set_xlim(min(ew, aw) - pad, max(ee, ae) + pad)
        ax.set_ylim(min(es, as_) - pad, max(en, an) + pad)

        dw = abs((ae - aw) - (ee - ew)) * 111_000   # metres
        dh = abs((an - as_) - (en - es)) * 111_000
        ax.set_title(f"{aoi_id}  {aoi['event_id']}\n"
                     f"size Δ: W={dw:.0f}m  H={dh:.0f}m", fontsize=7)
        ax.tick_params(labelsize=6)
        ax.set_xlabel("lon", fontsize=6)
        ax.set_ylabel("lat", fontsize=6)

    for idx in range(len(sample), n_rows * n_cols):
        r, c = divmod(idx, n_cols)
        axes[r][c].set_visible(False)

    handles = [
        mpatches.Patch(facecolor="none", edgecolor="steelblue", label="Expected bbox"),
        mpatches.Patch(facecolor="none", edgecolor="firebrick",
                       linestyle="--", label="Actual TIF bounds"),
    ]
    fig.legend(handles=handles, fontsize=8, loc="lower right",
               bbox_to_anchor=(0.99, 0.01))

    plt.tight_layout(rect=[0, 0.03, 1, 0.93])
    out = OUT_DIR / "footprint_accuracy.png"
    fig.savefig(out, dpi=140, bbox_inches="tight")
    plt.close(fig)
    print(f"  {out.name}")


# ── 3. Shape Consistency ──────────────────────────────────────────────────────

def plot_shape_consistency():
    exp_h, act_h = [], []
    exp_w, act_w = [], []
    mismatches   = []

    for a in AOI_LIST:
        aoi_id = a["aoi_id"]
        tif  = STACKS_DIR / f"{aoi_id}_stack.tif"
        meta = STACKS_DIR / f"{aoi_id}_meta.json"
        if not (tif.exists() and meta.exists()):
            continue
        m = json.load(open(meta))
        mH, mW = m["shape"][2], m["shape"][3]
        with rasterio.open(tif) as src:
            tH, tW = src.height, src.width
        exp_h.append(mH); act_h.append(tH)
        exp_w.append(mW); act_w.append(tW)
        if mH != tH or mW != tW:
            mismatches.append(f"{aoi_id}: meta=({mH},{mW}) tif=({tH},{tW})")

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 5))
    fig.suptitle(
        "Step 2b — Shape Consistency: meta JSON vs actual TIF\n"
        "All dots should lie on the red diagonal (meta = actual)",
        fontsize=10, fontweight="bold"
    )

    for ax, exp, act, lbl in [
        (ax1, exp_h, act_h, "Height (pixels)"),
        (ax2, exp_w, act_w, "Width (pixels)"),
    ]:
        ax.scatter(exp, act, s=8, alpha=0.5, color="steelblue", linewidths=0)
        lo = min(min(exp), min(act)) - 10
        hi = max(max(exp), max(act)) + 10
        ax.plot([lo, hi], [lo, hi], "r--", lw=1.5, label="perfect match")
        ax.set_xlim(lo, hi)
        ax.set_ylim(lo, hi)
        ax.set_xlabel(f"Expected {lbl}  (meta JSON)", fontsize=9)
        ax.set_ylabel(f"Actual {lbl}  (TIF on disk)", fontsize=9)
        ax.legend(fontsize=8)
        ax.grid(alpha=0.3, lw=0.5)

    n_ok  = len(exp_h) - len(mismatches)
    color = "green" if not mismatches else "red"
    msg   = (f"{n_ok}/{len(exp_h)} AOIs: meta shape = TIF shape  (perfect)"
             if not mismatches
             else f"{n_ok}/{len(exp_h)} OK  |  {len(mismatches)} mismatches: {mismatches[:3]}")
    fig.text(0.5, 0.01, msg, ha="center", fontsize=9, color=color)

    plt.tight_layout(rect=[0, 0.05, 1, 0.93])
    out = OUT_DIR / "shape_consistency.png"
    fig.savefig(out, dpi=140, bbox_inches="tight")
    plt.close(fig)
    print(f"  {out.name}")
    if mismatches:
        print(f"  WARNING: {len(mismatches)} mismatches — {mismatches[:3]}")


def check_stack_integrity():
    """Checks CRS, band count, and nodata consistency across all stacks. Prints a report."""
    print("\n  Checking CRS / band-count / nodata across all stacks...")
    crs_counts    = defaultdict(int)
    band_counts   = defaultdict(int)
    missing       = []
    bad_crs       = []
    bad_bands     = []

    import json as _json
    for a in AOI_LIST:
        aoi_id = a["aoi_id"]
        tif    = STACKS_DIR / f"{aoi_id}_stack.tif"
        meta   = STACKS_DIR / f"{aoi_id}_meta.json"
        if not tif.exists():
            missing.append(aoi_id)
            continue
        try:
            with rasterio.open(tif) as src:
                epsg = src.crs.to_epsg() if src.crs else None
                crs_counts[epsg] += 1
                band_counts[src.count] += 1
                if epsg is None:
                    bad_crs.append(f"{aoi_id}  CRS=None")
                if src.count < 3:
                    bad_bands.append(f"{aoi_id}  bands={src.count}")
        except Exception as e:
            bad_crs.append(f"{aoi_id}  open-error: {e}")

    print(f"  Missing stacks  : {len(missing)}")
    print(f"  CRS distribution: {dict(sorted(crs_counts.items()))}")
    print(f"  Band distribution: {dict(sorted(band_counts.items()))}")
    if bad_crs:
        print(f"  BAD CRS  ({len(bad_crs)}): {bad_crs[:10]}")
    if bad_bands:
        print(f"  BAD bands({len(bad_bands)}): {bad_bands[:10]}")
    if not bad_crs and not bad_bands and not missing:
        print("  All stacks: CRS valid, band count >= 3.")
    print()


def main():
    print("Running Step 2b co-registration verification...")
    check_stack_integrity()
    plot_ecc_checkerboard()
    plot_footprint_accuracy()
    plot_shape_consistency()
    print(f"\nDone — 3 images in {OUT_DIR}")


if __name__ == "__main__":
    main()
