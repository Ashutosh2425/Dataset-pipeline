"""
Step 3b — Flood Extent Extraction
Two sources (in priority order):
  1. Sen1Floods11 hand-labeled GT (remote GeoTIFFs via GCS)
  2. NDWI threshold on S2 bands (fallback — since S1 VV is unavailable)

Output: data/annotations/{aoi_id}/flood_extent_T{n}.geojson
"""

import json
from pathlib import Path

import numpy as np
import geopandas as gpd
import rasterio
import rasterio.features
from shapely.geometry import shape, box
from shapely.ops import unary_union
import cv2

AOI_LIST_PATH  = Path("data/aoi_list.json")
STACKS_DIR     = Path("data/stacks")
ANNOTATIONS    = Path("data/annotations")
SEN1_INDEX     = Path("data/sen1floods11/label_index.json")
SEN1_LABEL_DIR = Path("data/sen1floods11/v1.1/data/LabelHand")


# ── Sen1Floods11 local index ───────────────────────────────────────────────────

def build_sen1floods_index():
    """Loads pre-built local index of downloaded label TIFs."""
    if not SEN1_INDEX.exists():
        raise FileNotFoundError(
            f"Sen1Floods11 index not found: {SEN1_INDEX}\n"
            "Run: python scripts/download_sen1floods_labels.py"
        )
    with open(SEN1_INDEX) as f:
        return json.load(f)


def find_overlapping_chips(aoi_bbox, index):
    """Return chips whose bbox overlaps the AOI bbox."""
    aoi_geom = box(*aoi_bbox)
    return [c for c in index if box(*c['bbox']).intersects(aoi_geom)]


def extract_flood_from_sen1floods(aoi_id, aoi_bbox, index):
    """
    Reads overlapping local Sen1Floods11 label TIFs and merges flood polygons.
    Returns GeoDataFrame or None.
    """
    chips = find_overlapping_chips(aoi_bbox, index)
    if not chips:
        return None

    aoi_geom = box(*aoi_bbox)
    polygons = []
    crs = None

    for chip in chips:
        tif_path = Path(chip['path'])
        if not tif_path.exists():
            continue
        try:
            with rasterio.open(tif_path) as src:
                mask = src.read(1)
                transform = src.transform
                crs = src.crs

            flood_mask = (mask == 1).astype(np.uint8)
            for geom_dict, val in rasterio.features.shapes(
                flood_mask, mask=flood_mask, transform=transform
            ):
                g = shape(geom_dict)
                if g.intersects(aoi_geom):
                    polygons.append(g)
        except Exception as e:
            print(f"      [{aoi_id}] Sen1Floods chip error: {e}")
            continue

    if not polygons or crs is None:
        return None

    merged = unary_union(polygons)
    gdf = gpd.GeoDataFrame(
        [{'geometry': merged, 'source': 'sen1floods11_gt'}], crs=crs
    ).to_crs('EPSG:4326')
    return gdf.clip(box(*aoi_bbox))


# ── NDWI fallback (S2 Green + NIR) ────────────────────────────────────────────

def _read_band(stack_path, band_idx):
    with rasterio.open(stack_path) as src:
        return src.read(band_idx).astype(np.float32), src.transform, src.crs


def ndwi_flood(aoi_id, meta, stack_path, epoch_t):
    """
    Extracts flood extent using NDWI on S2 bands for a given epoch.
    NDWI = (Green - NIR) / (Green + NIR)  — positive = water
    Band layout per epoch (S2, 4 bands): B02, B03(green), B04, B08(nir)
    Memory-safe: frees arrays explicitly, simplifies before union.
    """
    bpe      = meta['bands_per_epoch']
    ep_start = sum(bpe[:epoch_t]) + 1

    if len(bpe) <= epoch_t or bpe[epoch_t] < 4:
        return None

    try:
        green_idx = ep_start + 1
        nir_idx   = ep_start + 3

        green, transform, crs = _read_band(stack_path, green_idx)
        nir, _, _             = _read_band(stack_path, nir_idx)

        denom = green + nir
        denom = np.where(denom == 0, 1e-6, denom)
        ndwi  = (green - nir) / denom
        del green, nir, denom

        ndwi_norm = ((ndwi - ndwi.min()) / (ndwi.max() - ndwi.min() + 1e-8) * 255).astype(np.uint8)
        del ndwi

        thresh, _ = cv2.threshold(ndwi_norm, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)
        flood_mask = (ndwi_norm >= thresh).astype(np.uint8)
        del ndwi_norm

        kernel = np.ones((3, 3), np.uint8)
        flood_mask = cv2.morphologyEx(flood_mask, cv2.MORPH_OPEN, kernel)

        # Collect and simplify polygons to cap memory usage
        polygons = []
        for geom_dict, _ in rasterio.features.shapes(flood_mask, mask=flood_mask, transform=transform):
            g = shape(geom_dict).simplify(0.0001)  # simplify before union
            if g.is_valid and not g.is_empty:
                polygons.append(g)
        del flood_mask

        if not polygons:
            return None

        # Union in batches of 500 to avoid OOM
        while len(polygons) > 1:
            batches = [polygons[i:i+500] for i in range(0, len(polygons), 500)]
            polygons = [unary_union(b) for b in batches]

        merged = polygons[0]
        gdf = gpd.GeoDataFrame(
            [{'geometry': merged, 'source': 'ndwi_otsu'}], crs=crs
        ).to_crs('EPSG:4326')
        return gdf

    except Exception as e:
        print(f"  [{aoi_id}] NDWI flood failed epoch {epoch_t}: {e}")
        return None


# ── Main pipeline ─────────────────────────────────────────────────────────────

class Step3bFloodPipeline:

    def __init__(self):
        with open(AOI_LIST_PATH) as f:
            self.aois = json.load(f)
        print("  Building Sen1Floods11 spatial index...")
        self.sen1_index = build_sen1floods_index()
        print(f"  Sen1Floods11 index: {len(self.sen1_index)} chips")

    def run(self):
        print(f"Step 3b: Flood extraction for {len(self.aois)} AOIs...")
        sen1_count = ndwi_count = skipped = 0

        for aoi in self.aois:
            aoi_id     = aoi['aoi_id']
            meta_path  = STACKS_DIR / f"{aoi_id}_meta.json"
            stack_path = STACKS_DIR / f"{aoi_id}_stack.tif"
            out_dir    = ANNOTATIONS / aoi_id
            out_dir.mkdir(parents=True, exist_ok=True)

            if not meta_path.exists():
                skipped += 1
                continue

            with open(meta_path) as f:
                meta = json.load(f)

            n_epochs = meta['n_epochs']

            all_done = all(
                (out_dir / f"flood_extent_T{t+1}.geojson").exists()
                for t in range(1, n_epochs)
            )
            if all_done:
                skipped += 1
                continue

            for t in range(1, n_epochs):
                out_path = out_dir / f"flood_extent_T{t+1}.geojson"
                if out_path.exists():
                    continue

                # Try Sen1Floods11 first
                flood_gdf = extract_flood_from_sen1floods(
                    aoi_id, aoi['bbox'], self.sen1_index
                )
                if flood_gdf is not None and len(flood_gdf) > 0:
                    flood_gdf['epoch'] = t + 1
                    flood_gdf.to_file(out_path, driver='GeoJSON')
                    sen1_count += 1
                    continue

                # NDWI fallback
                flood_gdf = ndwi_flood(aoi_id, meta, stack_path, t)
                if flood_gdf is not None and len(flood_gdf) > 0:
                    flood_gdf['epoch'] = t + 1
                    flood_gdf.to_file(out_path, driver='GeoJSON')
                    ndwi_count += 1
                else:
                    empty = gpd.GeoDataFrame(
                        columns=['geometry', 'source', 'epoch'],
                        geometry='geometry'
                    ).set_crs('EPSG:4326')
                    empty.to_file(out_path, driver='GeoJSON')

        print(f"\n{'='*50}")
        print(f"[Step 3b Complete]")
        print(f"  Sen1Floods11 GT: {sen1_count}")
        print(f"  NDWI fallback  : {ndwi_count}")
        print(f"  Skipped        : {skipped}")
        print(f"{'='*50}")
