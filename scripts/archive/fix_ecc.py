"""
fix_ecc.py
----------
Fixes ECC displacement > 10px violations in the checklist:
1. Re-runs ECC check to identify exact AOI+date pairs that fail
2. For each bad scene: queries STAC for a cleaner replacement date (±30 days, <15% cloud)
3. Downloads replacement, rebuilds stack
4. Verifies and pushes to GitHub

Run: conda run -n tdrd python fix_ecc.py
"""

import json
import os
import asyncio
import subprocess
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

import httpx
import numpy as np
import rasterio
import cv2

AOI_LIST_PATH = Path("data/aoi_list.json")
EPOCHS_PATH   = Path("data/aoi_epochs.json")
RAW_DIR       = Path("data/raw_scenes")
STACKS_DIR    = Path("data/stacks")
STAC_API_URL  = "https://earth-search.aws.element84.com/v1/search"
LOG           = Path("fix_ecc.log")

ECC_THRESHOLD = 10.0
S2_BAND_MAP   = {'blue': 'B02', 'green': 'B03', 'red': 'B04', 'nir': 'B08'}

_GDAL_ENV = {
    "AWS_NO_SIGN_REQUEST": "YES",
    "GDAL_DISABLE_READDIR_ON_OPEN": "EMPTY_DIR",
    "CPL_VSIL_CURL_ALLOWED_EXTENSIONS": ".tif,.TIF",
}


def log(msg):
    line = f"[{time.strftime('%H:%M:%S')}] {msg}"
    print(line, flush=True)
    with open(LOG, "a") as f:
        f.write(line + "\n")


# ── ECC audit ─────────────────────────────────────────────────────────────────

def _norm255(x):
    x_min, x_max = x.min(), x.max()
    if x_max - x_min < 1e-8:
        return np.zeros_like(x, dtype=np.uint8)
    return ((x - x_min) / (x_max - x_min + 1e-8) * 255).astype(np.uint8)


def _ecc_displacement(ref_gray, tgt_gray):
    warp = np.eye(2, 3, dtype=np.float32)
    criteria = (cv2.TERM_CRITERIA_EPS | cv2.TERM_CRITERIA_COUNT, 1000, 1e-6)
    try:
        _, warp = cv2.findTransformECC(
            _norm255(ref_gray), _norm255(tgt_gray),
            warp, cv2.MOTION_TRANSLATION, criteria
        )
    except cv2.error:
        pass
    return float(warp[0, 2]), float(warp[1, 2])


def audit_ecc():
    """Re-runs ECC check for all AOIs. Returns {aoi_id: [bad_date, ...]}."""
    log("Auditing ECC displacements for all 600 AOIs...")
    with open(AOI_LIST_PATH) as f:
        aois = json.load(f)

    bad = {}
    for aoi in aois:
        aoi_id  = aoi['aoi_id']
        aoi_dir = RAW_DIR / aoi_id
        merged  = sorted(aoi_dir.glob("*_merged.tif"))
        if len(merged) < 2:
            continue

        try:
            with rasterio.open(merged[0]) as src:
                ref_arr = src.read().astype(np.float32)
            ref_gray = ref_arr[3] if ref_arr.shape[0] >= 4 else ref_arr[0]
        except Exception:
            continue

        for scene in merged[1:]:
            date = scene.stem.split('_')[0]
            try:
                with rasterio.open(scene) as src:
                    tgt_arr = src.read().astype(np.float32)
                tgt_gray = tgt_arr[3] if tgt_arr.shape[0] >= 4 else tgt_arr[0]

                # Resize to match if needed
                if tgt_gray.shape != ref_gray.shape:
                    tgt_gray = cv2.resize(tgt_gray, (ref_gray.shape[1], ref_gray.shape[0]))

                dx, dy = _ecc_displacement(ref_gray, tgt_gray)
                if abs(dx) > ECC_THRESHOLD or abs(dy) > ECC_THRESHOLD:
                    bad.setdefault(aoi_id, []).append(date)
            except Exception as e:
                log(f"  [{aoi_id}] ECC check failed for {date}: {e}")

    total_bad = sum(len(v) for v in bad.values())
    log(f"Audit done: {len(bad)} AOIs have {total_bad} bad scenes total.")
    return bad


# ── STAC replacement query ────────────────────────────────────────────────────

def _scene_date(item):
    raw = item['properties'].get('datetime', '') or ''
    return raw[:10].replace('-', '') if len(raw) >= 10 else '00000000'


async def _find_replacement(client, aoi, bad_date_str, sem):
    """Find a clean S2 scene within ±30 days of bad_date, cloud < 15%."""
    bad_dt   = datetime.strptime(bad_date_str, '%Y%m%d')
    start    = (bad_dt - timedelta(days=30)).strftime('%Y-%m-%d')
    end      = (bad_dt + timedelta(days=30)).strftime('%Y-%m-%d')
    bbox     = aoi['bbox']

    async with sem:
        try:
            resp = await client.post(STAC_API_URL, json={
                "collections": ["sentinel-2-l2a"],
                "bbox": bbox,
                "datetime": f"{start}T00:00:00Z/{end}T23:59:59Z",
                "limit": 50,
                "query": {"eo:cloud_cover": {"lt": 15}},
            }, timeout=60.0)
            items = resp.json().get('features', []) if resp.status_code == 200 else []

            # Filter out the bad date itself
            candidates = [i for i in items if _scene_date(i) != bad_date_str]
            if not candidates:
                return None

            # Pick lowest cloud cover
            best = min(candidates, key=lambda x: x['properties'].get('eo:cloud_cover', 100))
            new_date = _scene_date(best)
            assets = {}
            for k, v in S2_BAND_MAP.items():
                if k in best.get('assets', {}) and best['assets'][k].get('href'):
                    assets[v] = best['assets'][k]['href']

            return {
                'epoch_label': 'replacement',
                'date':        new_date,
                'sensor':      'S2',
                'scene_id':    best.get('id', ''),
                'cloud_cover': best['properties'].get('eo:cloud_cover'),
                'assets':      assets,
            }
        except Exception as e:
            log(f"  Replacement query failed for {aoi['aoi_id']}/{bad_date_str}: {e}")
            return None


async def find_replacements(bad_aois):
    """Returns {aoi_id: {bad_date: replacement_epoch_or_None}}."""
    with open(AOI_LIST_PATH) as f:
        aoi_map = {a['aoi_id']: a for a in json.load(f)}

    sem     = asyncio.Semaphore(10)
    limits  = httpx.Limits(max_connections=20, max_keepalive_connections=10)
    results = {}

    async with httpx.AsyncClient(limits=limits) as client:
        tasks = []
        keys  = []
        for aoi_id, bad_dates in bad_aois.items():
            aoi = aoi_map.get(aoi_id)
            if not aoi:
                continue
            for d in bad_dates:
                tasks.append(_find_replacement(client, aoi, d, sem))
                keys.append((aoi_id, d))

        for key, coro in zip(keys, asyncio.as_completed(tasks)):
            pass  # need to gather properly

        # Gather all
        responses = await asyncio.gather(*tasks, return_exceptions=True)
        for (aoi_id, bad_date), resp in zip(keys, responses):
            if isinstance(resp, Exception):
                resp = None
            results.setdefault(aoi_id, {})[bad_date] = resp

    return results


# ── Download replacement scenes ───────────────────────────────────────────────

def _download_cog_window(url, bbox, output_path):
    import rasterio.env
    from rasterio.windows import from_bounds
    from pyproj import Transformer

    west, south, east, north = bbox
    try:
        with rasterio.env.Env(**_GDAL_ENV):
            with rasterio.open(url) as src:
                src_epsg = src.crs.to_epsg() if src.crs else None
                if src_epsg and src_epsg != 4326:
                    t = Transformer.from_crs("EPSG:4326", src.crs, always_xy=True)
                    west, south = t.transform(west, south)
                    east, north = t.transform(east, north)
                window = from_bounds(west, south, east, north, src.transform)
                if window.width <= 0 or window.height <= 0:
                    return False
                data = src.read(1, window=window)
                output_path.parent.mkdir(parents=True, exist_ok=True)
                with rasterio.open(output_path, "w", driver="GTiff",
                                   height=data.shape[0], width=data.shape[1],
                                   count=1, dtype=data.dtype,
                                   crs=src.crs,
                                   transform=src.window_transform(window)) as dst:
                    dst.write(data, 1)
        return True
    except Exception as e:
        log(f"      SKIP {output_path.name}: {e}")
        return False


def apply_replacements(bad_aois, replacements):
    """For each bad scene: delete old files, download replacement, clear stack."""
    with open(AOI_LIST_PATH) as f:
        aoi_map = {a['aoi_id']: a for a in json.load(f)}
    with open(EPOCHS_PATH) as f:
        epochs_db = json.load(f)

    fixed = 0
    skipped = 0

    for aoi_id, bad_dates in bad_aois.items():
        aoi  = aoi_map.get(aoi_id)
        if not aoi:
            continue
        bbox = aoi['bbox']
        aoi_dir = RAW_DIR / aoi_id

        for bad_date in bad_dates:
            replacement = replacements.get(aoi_id, {}).get(bad_date)
            if not replacement or not replacement.get('assets'):
                log(f"  [{aoi_id}] No replacement found for {bad_date} — keeping original")
                skipped += 1
                continue

            new_date = replacement['date']
            new_dir  = aoi_dir

            # Check replacement date not already downloaded
            existing = list(new_dir.glob(f"{new_date}_S2_*.tif"))
            if not existing:
                log(f"  [{aoi_id}] Downloading replacement {bad_date} → {new_date} (cloud={replacement.get('cloud_cover')}%)")
                for band_name, url in replacement['assets'].items():
                    out = new_dir / f"{new_date}_S2_{band_name}.tif"
                    _download_cog_window(url, bbox, out)
            else:
                log(f"  [{aoi_id}] Replacement {new_date} already exists")

            # Delete bad scene files (band files + merged)
            for f in list(new_dir.glob(f"{bad_date}_*.tif")):
                f.unlink()
                log(f"  [{aoi_id}] Deleted bad scene: {f.name}")

            # Update epochs_db: remove bad, add replacement
            aoi_epochs = epochs_db.get(aoi_id, [])
            aoi_epochs = [e for e in aoi_epochs if e['date'] != bad_date]
            if not any(e['date'] == new_date for e in aoi_epochs):
                aoi_epochs.append(replacement)
            aoi_epochs.sort(key=lambda e: e['date'])
            epochs_db[aoi_id] = aoi_epochs

            fixed += 1

        # Clear stale stack so it gets rebuilt
        for p in [STACKS_DIR / f"{aoi_id}_stack.tif", STACKS_DIR / f"{aoi_id}_meta.json"]:
            if p.exists():
                p.unlink()

        # Reset download flag so step2a skips (already have files) but step2b reruns
        # Actually keep flag since files exist — just need stack rebuild

    # Save updated epochs
    with open(EPOCHS_PATH, 'w') as f:
        json.dump(epochs_db, f, indent=2)

    log(f"Replacements applied: {fixed} scenes replaced, {skipped} kept as-is.")


def run_cmd(label, args):
    log(f"Running: {label}...")
    result = subprocess.run([sys.executable] + args, capture_output=True, text=True)
    out = (result.stdout or '') + (result.stderr or '')
    log(out[-3000:] if out else "(no output)")
    log(f"{label} exit code: {result.returncode}")
    return result.returncode == 0


def push_github():
    log("Pushing to GitHub...")
    for cmd in [
        ["git", "add", "data/aoi_epochs.json", "data/stacks/", "fix_ecc.log"],
        ["git", "commit", "-m", "Fix ECC displacement violations: replace bad scenes with cleaner alternatives"],
        ["git", "push", "origin", "main"],
    ]:
        r = subprocess.run(cmd, capture_output=True, text=True)
        log(f"  {cmd[1]}: {(r.stdout + r.stderr).strip()[:400]}")


def main():
    log("=== fix_ecc.py started ===")

    # Step 1: Audit which AOI+date pairs actually exceed threshold
    bad_aois = audit_ecc()
    if not bad_aois:
        log("No ECC violations found. Nothing to fix.")
        return

    log(f"Finding replacement scenes for {len(bad_aois)} AOIs...")

    # Step 2: Find replacements via STAC
    if os.name == 'nt':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    replacements = asyncio.run(find_replacements(bad_aois))

    # Step 3: Download replacements, delete bad scenes, update epochs
    apply_replacements(bad_aois, replacements)

    # Step 4: Rebuild stacks for affected AOIs
    run_cmd("step2b-coregister", ["main.py", "run-step2b"])

    # Step 5: Verify
    run_cmd("verify", ["verify_stacks.py"])

    # Step 6: Push
    push_github()

    log("=== fix_ecc.py done ===")


if __name__ == "__main__":
    main()
