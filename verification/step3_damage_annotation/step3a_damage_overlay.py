"""
Step 3a Verification — Damage Polygon Overlay
----------------------------------------------
For 2 AOIs per event, overlays damage polygons (coloured by damage_class)
on top of the RGB Sentinel-2 thumbnail for each post-event epoch.

  Class 1 (minor)    — yellow
  Class 2 (moderate) — orange
  Class 3 (destroyed)— red

Polygons are in WGS84; TIF is displayed in its WGS84 footprint so no
reprojection is needed for the overlay.

Output: verification/step3_damage_annotation/step3a_damage/EVT{N}_damage.png

Run from repo root:
    python verification/step3_damage_annotation/step3a_damage_overlay.py
"""

import json
import numpy as np
import rasterio
from rasterio.warp import transform_bounds
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.collections import PatchCollection
from pathlib import Path
from shapely.geometry import shape

BASE        = Path(__file__).resolve().parents[2]
STACKS_DIR  = BASE / "data" / "stacks"
ANN_DIR     = BASE / "data" / "annotations"
AOI_LIST    = json.load(open(BASE / "data" / "aoi_list.json"))
EPOCHS_MAP  = json.load(open(BASE / "data" / "aoi_epochs.json"))
OUT_DIR     = Path(__file__).parent / "step3a_damage"
OUT_DIR.mkdir(exist_ok=True)

N_PER_EVENT = 2

CLASS_COLORS = {1: "#f9d71c", 2: "#f97c1c", 3: "#e82020"}
CLASS_LABELS = {1: "minor", 2: "moderate", 3: "destroyed"}


def percentile_stretch(arr, lo=2, hi=98):
    valid = arr[arr > 0]
    if valid.size == 0:
        return np.zeros_like(arr, dtype=np.float32)
    vlo, vhi = np.percentile(valid, lo), np.percentile(valid, hi)
    if vhi <= vlo:
        return np.zeros_like(arr, dtype=np.float32)
    return np.clip((arr - vlo) / (vhi - vlo), 0, 1).astype(np.float32)


def read_rgb_epoch(tif_path, epoch_idx, bands_per_epoch):
    offset = sum(bands_per_epoch[:epoch_idx])
    with rasterio.open(tif_path) as src:
        blue  = src.read(offset + 1).astype(np.float32)
        green = src.read(offset + 2).astype(np.float32)
        red   = src.read(offset + 3).astype(np.float32)
        bounds_wgs84 = transform_bounds(src.crs, "EPSG:4326",
                                        *src.bounds)
    rgb = np.stack([percentile_stretch(red),
                    percentile_stretch(green),
                    percentile_stretch(blue)], axis=-1)
    return rgb, bounds_wgs84   # bounds = (west, south, east, north)


def load_damage_polygons(ann_dir, epoch_num):
    path = ann_dir / f"damage_polygons_T{epoch_num}.geojson"
    if not path.exists():
        return []
    data = json.load(open(path))
    return data.get("features", [])


def epoch_num_from_label(aoi_id, label):
    """Return 1-based epoch number matching the given label in aoi_epochs."""
    entries = EPOCHS_MAP.get(aoi_id, [])
    for i, ep in enumerate(entries):
        if ep["epoch_label"] == label:
            return i + 1
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
            ann  = ANN_DIR / a["aoi_id"]
            if tif.exists() and meta.exists() and ann.exists():
                m = json.load(open(meta))
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
        # Columns: during_1, during_2, during_3 (post-event epochs)
        epoch_labels = ["during_1", "during_2", "during_3"]
        n_rows = len(items)
        n_cols = len(epoch_labels)

        fig, axes = plt.subplots(n_rows, n_cols,
                                 figsize=(n_cols * 3.5, n_rows * 3.5),
                                 squeeze=False)
        fig.suptitle(
            f"{event_id}  ({event_type})  —  Damage Polygons per Epoch\n"
            "Yellow=minor  Orange=moderate  Red=destroyed  "
            "(source: xBD or spectral fallback)",
            fontsize=9, fontweight="bold"
        )

        for row, (aoi, meta) in enumerate(items):
            aoi_id  = aoi["aoi_id"]
            tif     = STACKS_DIR / f"{aoi_id}_stack.tif"
            ann_dir = ANN_DIR / aoi_id
            bpe     = meta["bands_per_epoch"]
            dates   = meta["dates"]

            for col, lbl in enumerate(epoch_labels):
                ax = axes[row][col]
                ep_num = epoch_num_from_label(aoi_id, lbl)

                if ep_num is None or ep_num > len(dates):
                    ax.set_visible(False)
                    continue

                ep_idx = ep_num - 1   # 0-based index into stack
                rgb, (west, south, east, north) = read_rgb_epoch(tif, ep_idx, bpe)

                ax.imshow(rgb, extent=[west, east, south, north],
                          origin="upper", aspect="auto")

                # Load and draw damage polygons
                features = load_damage_polygons(ann_dir, ep_num)
                for cls in [1, 2, 3]:
                    cls_feats = [f for f in features
                                 if f["properties"].get("damage_class") == cls]
                    patches = []
                    for feat in cls_feats:
                        try:
                            geom = shape(feat["geometry"])
                            if geom.is_empty:
                                continue
                            if geom.geom_type == "Polygon":
                                geoms = [geom]
                            else:
                                geoms = list(geom.geoms)
                            for g in geoms:
                                xs, ys = g.exterior.xy
                                patches.append(
                                    mpatches.Polygon(
                                        np.column_stack([xs, ys]),
                                        closed=True
                                    )
                                )
                        except Exception:
                            continue
                    if patches:
                        pc = PatchCollection(patches, facecolor=CLASS_COLORS[cls],
                                             alpha=0.45, edgecolor="none")
                        ax.add_collection(pc)

                source = "xBD" if features and features[0]["properties"].get("source") == "xbd" else "spectral"
                ax.set_title(f"{lbl}  {dates[ep_idx]}\n"
                             f"n={len(features)} polys  src={source}",
                             fontsize=7)
                ax.set_xlim(west, east)
                ax.set_ylim(south, north)
                ax.set_xticks([])
                ax.set_yticks([])

            axes[row][0].text(-0.15, 0.5, aoi_id,
                              transform=axes[row][0].transAxes,
                              fontsize=8, va="center", ha="right")

        # Legend
        handles = [mpatches.Patch(color=CLASS_COLORS[c], alpha=0.7,
                                   label=f"Class {c}: {CLASS_LABELS[c]}")
                   for c in [1, 2, 3]]
        fig.legend(handles=handles, fontsize=8, loc="lower right",
                   bbox_to_anchor=(0.99, 0.01), ncol=3)

        plt.tight_layout(rect=[0.06, 0.03, 1, 0.93])
        out = OUT_DIR / f"{event_id}_damage.png"
        fig.savefig(out, dpi=130, bbox_inches="tight")
        plt.close(fig)
        print(f"  {out.name}")

    print(f"\nDone — {len(by_event)} damage overlay PNGs in {OUT_DIR}")


if __name__ == "__main__":
    main()
