"""
Step 4a Verification — Road Damage Map on RGB Imagery
------------------------------------------------------
For 2 AOIs per event, overlays road segments coloured by damage status
on top of the RGB Sentinel-2 thumbnail for each post-event epoch.

  green  = passable   (score < 0.40)
  orange = degraded   (0.40 – 0.60)
  red    = impassable (score >= 0.60)
  gray   = road exists but no score for this epoch

Roads should visually align with flooded/damaged areas in the image.

Output: verification/step4_road_scoring/step4a_maps/EVT{N}_road_map.png

Run from repo root:
    python verification/step4_road_scoring/step4a_road_map.py
"""

import json
import sqlite3
import numpy as np
import rasterio
from rasterio.warp import transform_bounds
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import matplotlib.collections as mc
from pathlib import Path
from shapely.wkb import loads as wkb_loads

BASE        = Path(__file__).resolve().parents[2]
STACKS_DIR  = BASE / "data" / "stacks"
ANN_DIR     = BASE / "data" / "annotations"
AOI_LIST    = json.load(open(BASE / "data" / "aoi_list.json"))
EPOCHS_MAP  = json.load(open(BASE / "data" / "aoi_epochs.json"))
OUT_DIR     = Path(__file__).parent / "step4a_maps"
OUT_DIR.mkdir(exist_ok=True)

N_PER_EVENT  = 2
STATUS_COLOR = {"passable": "#27ae60", "degraded": "#e67e22", "impassable": "#e74c3c"}
NO_SCORE_COLOR = "#aaaaaa"

GPKG_HEADER_SIZES = {0: 0, 1: 32, 2: 48, 3: 48, 4: 64}


def parse_geom(blob):
    if not blob:
        return None
    try:
        env_type = (blob[3] >> 1) & 0x07
        offset = 8 + GPKG_HEADER_SIZES.get(env_type, 0)
        return wkb_loads(blob[offset:])
    except Exception:
        return None


def percentile_stretch(arr, lo=2, hi=98):
    valid = arr[arr > 0]
    if valid.size == 0:
        return np.zeros_like(arr, dtype=np.float32)
    vlo, vhi = np.percentile(valid, lo), np.percentile(valid, hi)
    if vhi <= vlo:
        return np.zeros_like(arr, dtype=np.float32)
    return np.clip((arr - vlo) / (vhi - vlo), 0, 1).astype(np.float32)


def read_rgb_epoch(tif_path, epoch_idx, bpe):
    offset = sum(bpe[:epoch_idx])
    with rasterio.open(tif_path) as src:
        blue  = src.read(offset + 1).astype(np.float32)
        green = src.read(offset + 2).astype(np.float32)
        red   = src.read(offset + 3).astype(np.float32)
        bounds_wgs84 = transform_bounds(src.crs, "EPSG:4326", *src.bounds)
    rgb = np.stack([percentile_stretch(red),
                    percentile_stretch(green),
                    percentile_stretch(blue)], axis=-1)
    return rgb, bounds_wgs84


def epoch_num_from_label(aoi_id, label):
    for i, ep in enumerate(EPOCHS_MAP.get(aoi_id, [])):
        if ep["epoch_label"] == label:
            return i + 1
    return None


def read_gpkg(gpkg_path):
    conn = sqlite3.connect(gpkg_path)
    cur  = conn.cursor()
    cols = [d[0] for d in cur.execute("SELECT * FROM road_damage_scores LIMIT 0").description]
    rows = cur.execute("SELECT * FROM road_damage_scores").fetchall()
    conn.close()
    return cols, rows


def draw_roads_on_ax(ax, cols, rows, score_col, status_col, west, east, south, north):
    geom_idx   = cols.index("geom")
    score_idx  = cols.index(score_col)  if score_col  in cols else None
    status_idx = cols.index(status_col) if status_col in cols else None

    buckets = {"passable": [], "degraded": [], "impassable": [], "none": []}

    for row in rows:
        geom = parse_geom(row[geom_idx])
        if geom is None:
            continue
        status = row[status_idx] if status_idx is not None else None

        if geom.geom_type == "LineString":
            segs_coords = [list(geom.coords)]
        elif geom.geom_type == "MultiLineString":
            segs_coords = [list(g.coords) for g in geom.geoms]
        else:
            continue

        for coords in segs_coords:
            xs = [c[0] for c in coords]
            ys = [c[1] for c in coords]
            # skip roads entirely outside the image extent
            if max(xs) < west or min(xs) > east or max(ys) < south or min(ys) > north:
                continue
            segs = list(zip(coords[:-1], coords[1:]))
            bucket = status if status in buckets else "none"
            buckets[bucket].extend(segs)

    # Draw in order: passable first (underneath), impassable on top
    for status in ["passable", "degraded", "impassable", "none"]:
        segs = buckets[status]
        if not segs:
            continue
        color = STATUS_COLOR.get(status, NO_SCORE_COLOR)
        lw = 1.5 if status == "impassable" else 1.0
        lc = mc.LineCollection(segs, colors=color, linewidths=lw, alpha=0.9, zorder=3)
        ax.add_collection(lc)


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
            gpkg = ANN_DIR / a["aoi_id"] / "road_damage_scores.gpkg"
            if tif.exists() and meta.exists() and gpkg.exists():
                # Prefer AOIs whose GPKG has roads
                cnt = sqlite3.connect(gpkg).execute(
                    "SELECT COUNT(*) FROM road_damage_scores"
                ).fetchone()[0]
                if cnt > 0:
                    m = json.load(open(meta))
                    valid.append((a, m, cnt))
        # Sort by road count descending so we pick AOIs with the most roads
        valid.sort(key=lambda x: -x[2])
        selected.extend(valid[:N_PER_EVENT])
    return selected


def main():
    sample = select_sample()
    by_event = {}
    for item in sample:
        by_event.setdefault(item[0]["event_id"], []).append(item)

    epoch_labels = ["during_1", "during_2", "during_3"]

    for event_id, items in sorted(by_event.items()):
        event_type = items[0][0]["event_type"]
        n_rows = len(items)
        n_cols = len(epoch_labels)

        fig, axes = plt.subplots(n_rows, n_cols,
                                 figsize=(n_cols * 4.0, n_rows * 4.0),
                                 squeeze=False)
        fig.suptitle(
            f"{event_id}  ({event_type})  —  Road Damage on RGB Imagery\n"
            "Green=passable  Orange=degraded  Red=impassable",
            fontsize=9, fontweight="bold"
        )

        for row, (aoi, meta, n_roads) in enumerate(items):
            aoi_id = aoi["aoi_id"]
            tif    = STACKS_DIR / f"{aoi_id}_stack.tif"
            gpkg   = ANN_DIR / aoi_id / "road_damage_scores.gpkg"
            bpe    = meta["bands_per_epoch"]
            dates  = meta["dates"]

            cols, rows_data = read_gpkg(gpkg)

            for col, lbl in enumerate(epoch_labels):
                ax = axes[row][col]
                ep_num = epoch_num_from_label(aoi_id, lbl)

                if ep_num is None or ep_num > len(dates):
                    ax.set_visible(False)
                    continue

                ep_idx = ep_num - 1
                try:
                    rgb, (west, south, east, north) = read_rgb_epoch(tif, ep_idx, bpe)
                except Exception:
                    ax.set_visible(False)
                    continue

                ax.imshow(rgb, extent=[west, east, south, north],
                          origin="upper", aspect="auto")

                score_col  = f"score_T{ep_num}"
                status_col = f"status_T{ep_num}"

                if score_col in cols:
                    draw_roads_on_ax(ax, cols, rows_data,
                                     score_col, status_col,
                                     west, east, south, north)
                    # Count statuses for subtitle
                    si = cols.index(status_col) if status_col in cols else None
                    if si is not None:
                        counts = {}
                        for r in rows_data:
                            s = r[si] or "none"
                            counts[s] = counts.get(s, 0) + 1
                        p  = counts.get("passable",   0)
                        d  = counts.get("degraded",   0)
                        im = counts.get("impassable", 0)
                        subtitle = f"P={p}  D={d}  I={im}"
                    else:
                        subtitle = f"{n_roads} roads"
                else:
                    subtitle = "no score data"

                ax.set_title(f"{lbl}  {dates[ep_idx]}\n{subtitle}", fontsize=7)
                ax.set_xlim(west, east)
                ax.set_ylim(south, north)
                ax.set_xticks([])
                ax.set_yticks([])

            axes[row][0].text(-0.12, 0.5, aoi_id,
                              transform=axes[row][0].transAxes,
                              fontsize=8, va="center", ha="right")

        handles = [
            mpatches.Patch(color=STATUS_COLOR["passable"],   label="Passable"),
            mpatches.Patch(color=STATUS_COLOR["degraded"],   label="Degraded"),
            mpatches.Patch(color=STATUS_COLOR["impassable"], label="Impassable"),
        ]
        fig.legend(handles=handles, fontsize=8, loc="lower right",
                   bbox_to_anchor=(0.99, 0.01), ncol=3)

        plt.tight_layout(rect=[0.07, 0.04, 1, 0.93])
        out = OUT_DIR / f"{event_id}_road_map.png"
        fig.savefig(out, dpi=140, bbox_inches="tight")
        plt.close(fig)
        print(f"  {out.name}")

    print(f"\nDone — {len(by_event)} road map PNGs in {OUT_DIR}")


if __name__ == "__main__":
    main()
