"""
Step 3b Verification — Flood Extent Overlay on NDWI
----------------------------------------------------
For 2 AOIs per event, overlays flood extent polygons (cyan outline)
on top of the NDWI heatmap for each post-event epoch.

Columns: during_1, during_2, during_3
The flood boundary should coincide with the blue (high NDWI) regions.

Output: verification/step3_damage_annotation/step3b_flood/EVT{N}_flood.png

Run from repo root:
    python verification/step3_damage_annotation/step3b_flood_overlay.py
"""

import json
import numpy as np
import rasterio
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
OUT_DIR     = Path(__file__).parent / "step3b_flood"
OUT_DIR.mkdir(exist_ok=True)

N_PER_EVENT = 2


def read_ndwi_epoch(tif_path, epoch_idx, bpe):
    if bpe[epoch_idx] < 4:
        return None, None
    offset = sum(bpe[:epoch_idx])
    with rasterio.open(tif_path) as src:
        green = src.read(offset + 2).astype(np.float32)
        nir   = src.read(offset + 4).astype(np.float32)
        from rasterio.warp import transform_bounds
        bounds_wgs84 = transform_bounds(src.crs, "EPSG:4326", *src.bounds)
    denom = green + nir
    ndwi = np.where(denom > 0, (green - nir) / denom, 0.0)
    return ndwi, bounds_wgs84


def epoch_num_from_label(aoi_id, label):
    entries = EPOCHS_MAP.get(aoi_id, [])
    for i, ep in enumerate(entries):
        if ep["epoch_label"] == label:
            return i + 1
    return None


def load_flood_polygons(ann_dir, epoch_num):
    path = ann_dir / f"flood_extent_T{epoch_num}.geojson"
    if not path.exists():
        return []
    data = json.load(open(path))
    return data.get("features", [])


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
        event_type  = items[0][0]["event_type"]
        epoch_labels = ["during_1", "during_2", "during_3"]
        n_rows = len(items)
        n_cols = len(epoch_labels)

        fig, axes = plt.subplots(n_rows, n_cols,
                                 figsize=(n_cols * 3.5, n_rows * 3.5),
                                 squeeze=False)
        fig.suptitle(
            f"{event_id}  ({event_type})  —  Flood Extent on NDWI\n"
            "NDWI: blue=water  red=land  |  Cyan outline = flood extent polygon",
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

                ep_idx = ep_num - 1
                ndwi, bounds = read_ndwi_epoch(tif, ep_idx, bpe)

                if ndwi is None:
                    ax.text(0.5, 0.5, "no B08", ha="center", va="center",
                            fontsize=9, transform=ax.transAxes, color="gray")
                    ax.set_facecolor("#f0f0f0")
                    ax.set_title(f"{lbl}  {dates[ep_idx]}", fontsize=7)
                    continue

                west, south, east, north = bounds
                ax.imshow(ndwi, cmap="RdBu", vmin=-0.5, vmax=0.5,
                          extent=[west, east, south, north],
                          origin="upper", aspect="auto")

                # Flood extent polygons
                features = load_flood_polygons(ann_dir, ep_num)
                patches = []
                for feat in features:
                    try:
                        geom = shape(feat["geometry"])
                        if geom.is_empty:
                            continue
                        if geom.geom_type == "Polygon":
                            geoms = [geom]
                        elif geom.geom_type == "MultiPolygon":
                            geoms = list(geom.geoms)
                        else:
                            geoms = []
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
                    pc = PatchCollection(patches, facecolor="none",
                                         edgecolor="cyan", linewidth=1.2, alpha=0.85)
                    ax.add_collection(pc)

                n_polys = sum(len(f.get("geometry", {}).get("coordinates", []))
                              for f in features)
                ax.set_title(f"{lbl}  {dates[ep_idx]}\n"
                             f"n={len(features)} flood feat  {n_polys} parts",
                             fontsize=7)
                ax.set_xlim(west, east)
                ax.set_ylim(south, north)
                ax.set_xticks([])
                ax.set_yticks([])

            axes[row][0].text(-0.15, 0.5, aoi_id,
                              transform=axes[row][0].transAxes,
                              fontsize=8, va="center", ha="right")

        # Shared colourbar
        fig.subplots_adjust(right=0.87, top=0.91)
        cb_ax = fig.add_axes([0.89, 0.2, 0.016, 0.55])
        sm = plt.cm.ScalarMappable(cmap="RdBu", norm=plt.Normalize(-0.5, 0.5))
        sm.set_array([])
        fig.colorbar(sm, cax=cb_ax).set_label("NDWI", fontsize=8)

        # Legend
        flood_patch = mpatches.Patch(facecolor="none", edgecolor="cyan",
                                     linewidth=1.5, label="Flood extent")
        fig.legend(handles=[flood_patch], fontsize=8, loc="lower right",
                   bbox_to_anchor=(0.88, 0.01))

        out = OUT_DIR / f"{event_id}_flood.png"
        fig.savefig(out, dpi=130, bbox_inches="tight")
        plt.close(fig)
        print(f"  {out.name}")

    print(f"\nDone — {len(by_event)} flood overlay PNGs in {OUT_DIR}")


if __name__ == "__main__":
    main()
