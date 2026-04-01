"""
Step 2a — Scene Download
Downloads windowed COG crops from Element84 STAC asset URLs (aoi_epochs.json).
Output: data/raw_scenes/{aoi_id}/{date}_{sensor}_{band}.tif
No credentials required.
"""

import os
import json
import time
from pathlib import Path

import numpy as np
import rasterio
import rasterio.env
from rasterio.windows import from_bounds
from pyproj import Transformer
from tqdm import tqdm

from tdrd.config import AOI_LIST_PATH

EPOCHS_PATH  = "data/aoi_epochs.json"
MIN_EPOCHS   = 3   # Minimum dated folders with at least one .tif to flag an AOI complete

# GDAL env for anonymous S3 access.
# sentinel-cogs (S2) is public → succeeds.
# sentinel-s1-l1c (S1) is requester-pays → fails fast instead of hanging.
_GDAL_ENV = {
    "AWS_NO_SIGN_REQUEST":          "YES",
    "GDAL_DISABLE_READDIR_ON_OPEN": "EMPTY_DIR",
    "CPL_VSIL_CURL_ALLOWED_EXTENSIONS": ".tif,.TIF",
}


def _download_cog_window(url: str, bbox: list, output_path: Path) -> bool:
    """Opens a remote COG and writes a windowed crop for the given bbox. Returns True on success."""
    west, south, east, north = bbox

    try:
        with rasterio.env.Env(**_GDAL_ENV):
            with rasterio.open(url) as src:
                src_epsg = src.crs.to_epsg() if src.crs else None
                if src_epsg and src_epsg != 4326:
                    t = Transformer.from_crs("EPSG:4326", src.crs, always_xy=True)
                    west_t, south_t = t.transform(west, south)
                    east_t, north_t = t.transform(east, north)
                else:
                    west_t, south_t, east_t, north_t = west, south, east, north

                window = from_bounds(west_t, south_t, east_t, north_t, src.transform)

                if window.width <= 0 or window.height <= 0:
                    return False

                data = src.read(1, window=window)
                win_transform = src.window_transform(window)

                output_path.parent.mkdir(parents=True, exist_ok=True)
                with rasterio.open(
                    output_path, "w",
                    driver="GTiff",
                    height=data.shape[0],
                    width=data.shape[1],
                    count=1,
                    dtype=data.dtype,
                    crs=src.crs,
                    transform=win_transform,
                ) as dst:
                    dst.write(data, 1)

        return True

    except Exception as e:
        print(f"      SKIP {output_path.name}: {e}")
        return False


class Step2aPipeline:
    """Downloads windowed COG crops for every epoch in aoi_epochs.json. Resumes from flag."""

    def __init__(self, raw_dir: str = "data/raw_scenes"):
        self.raw_dir = Path(raw_dir)
        self.raw_dir.mkdir(parents=True, exist_ok=True)
        self.aois   = self._load_aois()
        self.epochs = self._load_epochs()

    def _load_aois(self) -> list:
        if not os.path.exists(AOI_LIST_PATH):
            raise FileNotFoundError(f"AOI list not found: {AOI_LIST_PATH}")
        with open(AOI_LIST_PATH) as f:
            return json.load(f)

    def _load_epochs(self) -> dict:
        if not os.path.exists(EPOCHS_PATH):
            raise FileNotFoundError(
                f"Epoch index not found: {EPOCHS_PATH}\n"
                "Run 'python main.py run-step2a-query' first."
            )
        with open(EPOCHS_PATH) as f:
            return json.load(f)

    def _download_aoi(self, aoi: dict) -> bool:
        """
        Downloads all epoch bands for a single AOI.
        S1 bands may fail (requester-pays bucket) — that is tolerated.
        Returns True if at least MIN_EPOCHS dated folders contain at least one .tif.
        """
        aoi_id  = aoi["aoi_id"]
        bbox    = aoi["bbox"]
        aoi_dir = self.raw_dir / aoi_id

        epochs = self.epochs.get(aoi_id, [])
        if not epochs:
            return False

        for epoch in epochs:
            date   = epoch["date"]
            sensor = epoch["sensor"]
            for band_name, url in epoch.get("assets", {}).items():
                out_file = aoi_dir / f"{date}_{sensor}_{band_name}.tif"
                if out_file.exists():
                    continue
                _download_cog_window(url, bbox, out_file)
            time.sleep(0.05)

        # Count distinct dates that have at least one downloaded file
        dates_with_data = {
            f.stem.split("_")[0]
            for f in aoi_dir.glob("*.tif")
        }
        return len(dates_with_data) >= MIN_EPOCHS

    def run(self):
        print(f"Step 2a: Downloading COG crops for {len(self.aois)} AOIs...")
        print(f"  Source : {EPOCHS_PATH}")
        print(f"  Output : {self.raw_dir}")
        print(f"  Note   : S1 bands skipped if bucket is requester-pays (S2 sufficient)\n")

        skipped   = 0
        completed = 0
        failed    = 0

        for aoi in tqdm(self.aois, desc="Downloading"):
            aoi_id  = aoi["aoi_id"]
            aoi_dir = self.raw_dir / aoi_id
            flag    = aoi_dir / "download_complete.flag"

            if flag.exists():
                skipped += 1
                continue

            aoi_dir.mkdir(parents=True, exist_ok=True)
            ok = self._download_aoi(aoi)

            if ok:
                flag.write_text("done")
                completed += 1
            else:
                failed += 1

        print(f"\n{'='*50}")
        print(f"[Step 2a Complete]")
        print(f"  Completed : {completed}")
        print(f"  Skipped   : {skipped}  (already done)")
        print(f"  Failed    : {failed}  (< {MIN_EPOCHS} usable epochs)")
        print(f"{'='*50}")
