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
from concurrent.futures import ThreadPoolExecutor, as_completed

import numpy as np
import rasterio
import rasterio.env
from rasterio.windows import from_bounds
from pyproj import Transformer
from tqdm import tqdm

from tdrd.config import AOI_LIST_PATH

EPOCHS_PATH  = "data/aoi_epochs.json"
MIN_EPOCHS   = 3
WORKERS      = 12   # reduced from 30 — 30 caused DNS exhaustion

# GDAL env for anonymous S3 access.
# sentinel-cogs (S2) is public → succeeds.
# sentinel-s1-l1c (S1) is requester-pays → fails fast instead of hanging.
_GDAL_ENV = {
    "AWS_NO_SIGN_REQUEST":          "YES",
    "GDAL_DISABLE_READDIR_ON_OPEN": "EMPTY_DIR",
    "CPL_VSIL_CURL_ALLOWED_EXTENSIONS": ".tif,.TIF",
}


def _download_cog_window(url: str, bbox: list, output_path: Path, retries: int = 3) -> bool:
    """Opens a remote COG and writes a windowed crop for the given bbox. Retries on DNS/network errors."""
    west, south, east, north = bbox

    for attempt in range(retries):
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
            err = str(e)
            if attempt < retries - 1 and ("resolve host" in err or "CURL" in err or "timed out" in err.lower()):
                time.sleep(2 ** attempt)   # 1s, 2s backoff
                continue
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

    def _build_task_list(self):
        """Build a flat list of (aoi_id, bbox, out_file, url) for every band that needs downloading."""
        tasks = []
        for aoi in self.aois:
            aoi_id  = aoi["aoi_id"]
            bbox    = aoi["bbox"]
            aoi_dir = self.raw_dir / aoi_id
            if (aoi_dir / "download_complete.flag").exists():
                continue
            for epoch in self.epochs.get(aoi_id, []):
                date   = epoch["date"]
                sensor = epoch["sensor"]
                for band_name, url in epoch.get("assets", {}).items():
                    out_file = aoi_dir / f"{date}_{sensor}_{band_name}.tif"
                    if not out_file.exists():
                        tasks.append((aoi_id, bbox, out_file, url))
        return tasks

    def run(self):
        pending_aois = [
            a for a in self.aois
            if not (self.raw_dir / a["aoi_id"] / "download_complete.flag").exists()
        ]
        skipped = len(self.aois) - len(pending_aois)
        print(f"Step 2a: {len(pending_aois)} AOIs to download  ({skipped} already done)")
        print(f"  Workers : {WORKERS} parallel threads")
        print(f"  Source  : {EPOCHS_PATH}")
        print(f"  Output  : {self.raw_dir}\n")

        if not pending_aois:
            print("Nothing to do.")
            return

        # Pre-create AOI directories
        for aoi in pending_aois:
            (self.raw_dir / aoi["aoi_id"]).mkdir(parents=True, exist_ok=True)

        tasks = self._build_task_list()
        print(f"  Total band files to download: {len(tasks)}\n")

        success_count = 0
        fail_count    = 0

        with ThreadPoolExecutor(max_workers=WORKERS) as pool:
            futures = {
                pool.submit(_download_cog_window, url, bbox, out_file): aoi_id
                for aoi_id, bbox, out_file, url in tasks
            }
            with tqdm(total=len(futures), desc="Bands", unit="file") as pbar:
                for fut in as_completed(futures):
                    ok = fut.result()
                    if ok:
                        success_count += 1
                    else:
                        fail_count += 1
                    pbar.update(1)

        # Write completion flags
        completed = failed = 0
        for aoi in pending_aois:
            aoi_id  = aoi["aoi_id"]
            aoi_dir = self.raw_dir / aoi_id
            dates   = {f.stem.split("_")[0] for f in aoi_dir.glob("*.tif")}
            if len(dates) >= MIN_EPOCHS:
                (aoi_dir / "download_complete.flag").write_text("done")
                completed += 1
            else:
                failed += 1

        print(f"\n{'='*50}")
        print(f"[Step 2a Complete]")
        print(f"  Band files OK    : {success_count}")
        print(f"  Band files SKIP  : {fail_count}  (S1 requester-pays or no data)")
        print(f"  AOIs completed   : {completed}")
        print(f"  AOIs skipped     : {skipped}")
        print(f"  AOIs failed      : {failed}  (< {MIN_EPOCHS} usable epochs)")
        print(f"{'='*50}")
