"""
tdrd/pipelines/coregister.py
----------------------------
Step 2b: Co-registers downloaded Sentinel scenes for each AOI into a temporal stack.
Uses ECC (Enhanced Correlation Coefficient) for sub-pixel alignment.
"""

import os
import json
import numpy as np
import rasterio
import cv2
from pathlib import Path
from rasterio.warp import reproject, Resampling
from tdrd.config import AOI_LIST_PATH
from tqdm import tqdm

class Step2bPipeline:
    """
    Handles the Step 2b (Temporal Stack Construction) logic.
    """
    
    def __init__(self, raw_dir='data/raw_scenes', output_dir='data/stacks'):
        self.raw_dir = Path(raw_dir)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.aois = self._load_aois()

    def _load_aois(self):
        if not os.path.exists(AOI_LIST_PATH):
            raise FileNotFoundError(f"AOI list not found at {AOI_LIST_PATH}")
        with open(AOI_LIST_PATH, 'r') as f:
            return json.load(f)

    @staticmethod
    def load_scene(scene_path, bands=None):
        """Loads a GeoTIFF scene as a numpy array."""
        with rasterio.open(scene_path) as src:
            if bands:
                arr = src.read(bands)
            else:
                arr = src.read()
            return arr.astype(np.float32), src.transform, src.crs, src.meta

    @staticmethod
    def reproject_to_reference(src_arr, src_transform, src_crs, ref_transform, ref_crs, ref_shape):
        """Reprojects a numpy array to match a reference grid."""
        n_bands = src_arr.shape[0]
        reprojected = np.zeros((n_bands, ref_shape[0], ref_shape[1]), dtype=np.float32)
        for b in range(n_bands):
            reproject(
                source=src_arr[b], destination=reprojected[b],
                src_transform=src_transform, src_crs=src_crs,
                dst_transform=ref_transform, dst_crs=ref_crs,
                resampling=Resampling.bilinear
            )
        return reprojected

    @staticmethod
    def ecc_align(reference_gray, target_gray, max_iter=1000, epsilon=1e-6):
        """Computes ECC matrix for sub-pixel translation alignment."""
        def norm255(x):
            x_min = x.min()
            x_max = x.max()
            if x_max - x_min < 1e-8:
                return np.zeros_like(x, dtype=np.uint8)
            return ((x - x_min) / (x_max - x_min + 1e-8) * 255).astype(np.uint8)

        ref_u8 = norm255(reference_gray)
        tgt_u8 = norm255(target_gray)

        warp_matrix = np.eye(2, 3, dtype=np.float32)
        criteria = (cv2.TERM_CRITERIA_EPS | cv2.TERM_CRITERIA_COUNT, max_iter, epsilon)

        try:
            _, warp_matrix = cv2.findTransformECC(
                ref_u8, tgt_u8, warp_matrix,
                cv2.MOTION_TRANSLATION, criteria
            )
        except cv2.error as e:
            # Fallback to identity if ECC fails
            pass

        return warp_matrix

    @staticmethod
    def apply_warp(arr, warp_matrix):
        """Applies an affine warp to a multi-band numpy array."""
        H, W = arr.shape[-2], arr.shape[-1]
        warped = np.zeros_like(arr)
        for b in range(arr.shape[0]):
            warped[b] = cv2.warpAffine(
                arr[b], warp_matrix, (W, H),
                flags=cv2.INTER_LINEAR + cv2.WARP_INVERSE_MAP
            )
        return warped

    @staticmethod
    def _date_from_merged(path: Path) -> str:
        """Extracts YYYYMMDD from a merged filename like 20170901_merged.tif."""
        return path.stem.split('_')[0]

    def build_temporal_stack(self, aoi_id, scene_paths):
        """Constructs a co-registered stack from list of per-epoch merged scene files."""
        # Sort by date (first 8 chars of stem = YYYYMMDD)
        scene_paths.sort(key=lambda p: self._date_from_merged(p))

        # Reference scene (earliest / pre-event epoch)
        ref_arr, ref_transform, ref_crs, ref_meta = self.load_scene(scene_paths[0])
        H, W = ref_arr.shape[-2:]
        ref_gray = ref_arr[3] if ref_arr.shape[0] >= 4 else ref_arr[0]

        aligned_stack = [ref_arr]
        dates         = [self._date_from_merged(scene_paths[0])]

        for scene_path in scene_paths[1:]:
            arr, transform, crs, _ = self.load_scene(scene_path)

            # Reproject to reference grid
            reproj_arr = self.reproject_to_reference(arr, transform, crs, ref_transform, ref_crs, (H, W))

            # ECC sub-pixel alignment
            tgt_gray = reproj_arr[3] if reproj_arr.shape[0] >= 4 else reproj_arr[0]
            warp = self.ecc_align(ref_gray, tgt_gray)
            arr_aligned = self.apply_warp(reproj_arr, warp)

            dx, dy = warp[0, 2], warp[1, 2]
            if abs(dx) > 10 or abs(dy) > 10:
                print(f"    WARNING: large ECC displacement ({dx:.1f}, {dy:.1f}) for {scene_path.name}")

            aligned_stack.append(arr_aligned)
            dates.append(self._date_from_merged(scene_path))

        # Write stack — each epoch may have a different band count (S1=2, S2=4)
        # so we concatenate bands sequentially and track per-epoch counts in meta.
        bands_per_epoch = [e.shape[0] for e in aligned_stack]
        total_bands     = sum(bands_per_epoch)
        stack_path      = self.output_dir / f"{aoi_id}_stack.tif"

        with rasterio.open(stack_path, 'w', **{**ref_meta, 'count': total_bands}) as dst:
            cursor = 1
            for epoch_arr in aligned_stack:
                for b in range(epoch_arr.shape[0]):
                    dst.write(epoch_arr[b], cursor)
                    cursor += 1

        meta_path = self.output_dir / f"{aoi_id}_meta.json"
        meta_info = {
            'aoi_id':          aoi_id,
            'dates':           dates,
            'n_epochs':        len(dates),
            'bands_per_epoch': bands_per_epoch,
            'shape':           [len(dates), max(bands_per_epoch), H, W],
        }
        with open(meta_path, 'w') as f:
            json.dump(meta_info, f, indent=2)

        return str(stack_path)

    def run(self):
        """Groups downloaded band files by date, merges per epoch, builds ECC-aligned stacks."""
        print(f"Starting Step 2b: Co-registration for {len(self.aois)} AOIs...")

        skipped   = 0
        completed = 0
        failed    = 0

        for aoi in tqdm(self.aois, desc="Stacking Progress"):
            aoi_id      = aoi['aoi_id']
            aoi_raw_dir = self.raw_dir / aoi_id
            stack_path  = self.output_dir / f"{aoi_id}_stack.tif"
            meta_path   = self.output_dir / f"{aoi_id}_meta.json"

            # Skip if already stacked
            if stack_path.exists() and meta_path.exists():
                skipped += 1
                continue

            if not aoi_raw_dir.exists():
                continue

            # Collect all .tif files in this AOI's raw dir
            tif_files = sorted(aoi_raw_dir.glob("*.tif"))
            if not tif_files:
                continue

            # Group by date: {date: [path, ...]}
            date_groups: dict = {}
            for tif in tif_files:
                # filename format: {date}_{sensor}_{band}.tif
                parts = tif.stem.split("_")
                if len(parts) < 3:
                    continue
                date = parts[0]
                date_groups.setdefault(date, []).append(tif)

            if len(date_groups) < 3:
                print(f"  [{aoi_id}] Only {len(date_groups)} epochs — need >= 3, skipping.")
                continue

            # Build one merged GeoTIFF per epoch (all bands stacked as multi-band)
            epoch_files = []
            for date in sorted(date_groups.keys()):
                band_files = sorted(date_groups[date])
                merged_path = aoi_raw_dir / f"{date}_merged.tif"

                if not merged_path.exists():
                    try:
                        self._merge_bands(band_files, merged_path)
                    except Exception as e:
                        print(f"  [{aoi_id}] Merge failed for {date}: {e}")
                        continue

                epoch_files.append(merged_path)

            if len(epoch_files) < 3:
                failed += 1
                continue

            try:
                self.build_temporal_stack(aoi_id, epoch_files)
                completed += 1
            except Exception as e:
                print(f"  [{aoi_id}] Stack build failed: {e}")
                failed += 1

        print(f"\n{'='*50}")
        print(f"[Step 2b Complete]")
        print(f"  Completed : {completed}")
        print(f"  Skipped   : {skipped}  (already done)")
        print(f"  Failed    : {failed}")
        print(f"{'='*50}")

    @staticmethod
    def _merge_bands(band_files: list, output_path: Path):
        """Merges single-band GeoTIFFs into one multi-band file, reprojecting to the first file's grid."""
        # Use first file as reference grid
        with rasterio.open(band_files[0]) as ref:
            ref_meta = ref.meta.copy()
            ref_transform = ref.transform
            ref_crs = ref.crs
            H, W = ref.height, ref.width

        bands = []
        for bf in band_files:
            with rasterio.open(bf) as src:
                arr = src.read(1).astype(np.float32)
                if src.crs != ref_crs or src.transform != ref_transform:
                    from rasterio.warp import reproject, Resampling
                    dest = np.zeros((H, W), dtype=np.float32)
                    reproject(
                        source=arr, destination=dest,
                        src_transform=src.transform, src_crs=src.crs,
                        dst_transform=ref_transform, dst_crs=ref_crs,
                        resampling=Resampling.bilinear
                    )
                    arr = dest
            bands.append(arr)

        ref_meta.update(count=len(bands), dtype='float32')
        with rasterio.open(output_path, 'w', **ref_meta) as dst:
            for i, band in enumerate(bands, start=1):
                dst.write(band, i)

