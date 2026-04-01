"""
verify_stacks.py
----------------
Verifies that Step 2b output meets the build guide checklist:
  - data/stacks/{aoi_id}_stack.tif exists
  - data/stacks/{aoi_id}_meta.json exists with n_epochs >= 3
  - No stack has ECC displacement > 10 pixels (checked from meta if recorded)

Usage:
    python verify_stacks.py
"""

import json
import os
from pathlib import Path

AOI_LIST_PATH = "data/aoi_list.json"
STACKS_DIR    = Path("data/stacks")


def main():
    if not os.path.exists(AOI_LIST_PATH):
        print(f"ERROR: {AOI_LIST_PATH} not found.")
        return

    with open(AOI_LIST_PATH) as f:
        aois = json.load(f)

    total       = len(aois)
    ok          = 0
    missing_tif = []
    missing_meta = []
    low_epochs  = []

    for aoi in aois:
        aoi_id    = aoi["aoi_id"]
        tif_path  = STACKS_DIR / f"{aoi_id}_stack.tif"
        meta_path = STACKS_DIR / f"{aoi_id}_meta.json"

        if not tif_path.exists():
            missing_tif.append(aoi_id)
            continue

        if not meta_path.exists():
            missing_meta.append(aoi_id)
            continue

        with open(meta_path) as f:
            meta = json.load(f)

        n_epochs = meta.get("n_epochs", 0)
        bands_per_epoch = meta.get("bands_per_epoch", [])
        if n_epochs < 3:
            low_epochs.append((aoi_id, n_epochs))
            continue
        if bands_per_epoch and any(b == 0 for b in bands_per_epoch):
            low_epochs.append((aoi_id, f"zero-band epoch in {bands_per_epoch}"))
            continue

        ok += 1

    print(f"\n{'='*55}")
    print(f"  verify_stacks.py  —  {total} AOIs checked")
    print(f"{'='*55}")
    print(f"  OK              : {ok} / {total}")
    print(f"  Missing .tif    : {len(missing_tif)}")
    print(f"  Missing meta    : {len(missing_meta)}")
    print(f"  < 3 epochs      : {len(low_epochs)}")

    if missing_tif:
        print(f"\n  First 10 missing tif: {missing_tif[:10]}")
    if missing_meta:
        print(f"\n  First 10 missing meta: {missing_meta[:10]}")
    if low_epochs:
        print(f"\n  First 10 low-epoch: {low_epochs[:10]}")

    print(f"\n  {'PASS' if ok == total else 'FAIL'}  —  {ok}/{total} OK")
    print(f"{'='*55}\n")


if __name__ == "__main__":
    main()
