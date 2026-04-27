"""
Rebuilds AOI selection for the 6 events whose xBD folder was mismatched.

Changed events:
  EVT003  Camp Fire          → Santa Rosa Wildfire 2017
  EVT005  Midwest Floods     → Midwest Flooding (corrected bbox)
  EVT008  Natchez Tornadoes  → Hurricane Michael 2018
  EVT009  Pakistan Floods    → Nepal Flooding 2017
  EVT010  Turkey Earthquake  → Mexico Earthquake 2017
  EVT012  Libya Derna Floods → Hurricane Florence 2018

What this script does:
  1. Backs up current aoi_list.json
  2. Strips the 6 changed events from aoi_list.json (keeps the other 6)
  3. Clears STAC checkpoints for changed events
  4. Deletes raw_scenes + stacks + flags for old AOIs of changed events
  5. Runs Step 1 for the 6 missing events (STAC → xBD → OSMnx)
  6. Re-sorts by EVENTS order and reassigns IDs globally
  7. Updates aoi_epochs.json (removes old entries for changed events)

Run from repo root:
    python scripts/rebuild_xbd_events.py
"""

import json
import asyncio
import os
import shutil
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from tdrd.config import EVENTS, EVENT_TARGETS, AOI_LIST_PATH
from tdrd.pipelines.step1_select_aois import Step1Pipeline

CHANGED_EVENTS = {'EVT003', 'EVT005', 'EVT008', 'EVT009', 'EVT010', 'EVT012'}
UNCHANGED_EVENTS = {'EVT001', 'EVT002', 'EVT004', 'EVT006', 'EVT007', 'EVT011'}

AOI_LIST   = Path(AOI_LIST_PATH)
BACKUP     = Path("data/aoi_list_backup_prerebuilt.json")
EPOCHS     = Path("data/aoi_epochs.json")
RAW_DIR    = Path("data/raw_scenes")
STACKS_DIR = Path("data/stacks")
CKPT_DIR   = Path("data/step1_checkpoints")


def backup_aoi_list():
    if not BACKUP.exists():
        shutil.copy(AOI_LIST, BACKUP)
        print(f"Backed up aoi_list.json -> {BACKUP}")
    else:
        print(f"Backup already exists at {BACKUP}, skipping")


def find_old_aoi_ids(aois):
    return {a['aoi_id'] for a in aois if a['event_id'] in CHANGED_EVENTS}


def delete_old_data(old_ids):
    print(f"\nDeleting data for {len(old_ids)} old AOIs...")
    deleted_raw = deleted_stack = deleted_meta = 0
    for aoi_id in sorted(old_ids):
        raw_dir = RAW_DIR / aoi_id
        if raw_dir.exists():
            def _force_rm(func, path, _):
                os.chmod(path, 0o777)
                func(path)
            shutil.rmtree(raw_dir, onerror=_force_rm)
            deleted_raw += 1
        stack = STACKS_DIR / f"{aoi_id}_stack.tif"
        meta  = STACKS_DIR / f"{aoi_id}_meta.json"
        if stack.exists():
            stack.unlink(); deleted_stack += 1
        if meta.exists():
            meta.unlink(); deleted_meta += 1
    print(f"  Deleted {deleted_raw} raw_scene dirs, {deleted_stack} stacks, {deleted_meta} metas")


def clear_checkpoints():
    for eid in CHANGED_EVENTS:
        ckpt = CKPT_DIR / f"stac_{eid}.json"
        if ckpt.exists():
            ckpt.unlink()
            print(f"  Cleared checkpoint: {ckpt.name}")


def strip_changed_events(aois):
    kept = [a for a in aois if a['event_id'] in UNCHANGED_EVENTS]
    print(f"Kept {len(kept)} AOIs from unchanged events")
    return kept


def run_step1_for_changed(kept_aois):
    """Runs Step 1 pipeline only for the 6 changed events, using the kept AOIs as the starting point."""
    # Write stripped list so Step1Pipeline loads it
    with open(AOI_LIST, 'w') as f:
        json.dump(kept_aois, f, indent=2)

    print("\nRunning Step 1 for changed events...")
    pipeline = Step1Pipeline()
    pipeline.run()

    # Load the result (Step1Pipeline saves after each event)
    with open(AOI_LIST) as f:
        return json.load(f)


def sort_and_reindex(aois):
    """Sort AOIs by EVENTS order (stable within each event), then reassign sequential IDs."""
    event_order = {e['id']: i for i, e in enumerate(EVENTS)}
    for j, a in enumerate(aois):
        a['_orig_idx'] = j
    aois.sort(key=lambda a: (event_order.get(a['event_id'], 99), a['_orig_idx']))
    for a in aois:
        del a['_orig_idx']
    for i, a in enumerate(aois):
        a['aoi_id'] = f"{a['event_id']}_{i:04d}"
    return aois


def update_epochs(old_ids):
    if not EPOCHS.exists():
        return
    epochs = json.load(open(EPOCHS))
    for aoi_id in old_ids:
        epochs.pop(aoi_id, None)
    with open(EPOCHS, 'w') as f:
        json.dump(epochs, f, indent=2)
    print(f"Removed {len(old_ids)} old epoch entries from aoi_epochs.json")


def print_xbd_summary(aois):
    from collections import Counter
    xbd_true  = sum(1 for a in aois if a.get('has_xbd_overlap'))
    xbd_false = len(aois) - xbd_true
    by_event  = Counter((a['event_id'], a.get('has_xbd_overlap')) for a in aois)
    print(f"\nxBD coverage: {xbd_true}/{len(aois)} AOIs = {100*xbd_true/len(aois):.1f}%")
    for eid in sorted({a['event_id'] for a in aois}):
        t = by_event[(eid, True)]
        f = by_event[(eid, False)]
        flag = "xBD" if t else "spectral"
        print(f"  {eid}: {t+f} AOIs  ({flag})")


def main():
    if os.name == 'nt':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    print("=" * 60)
    print("Rebuilding 6 xBD-mismatched events")
    print("=" * 60)

    existing = json.load(open(AOI_LIST))
    print(f"Current aoi_list.json: {len(existing)} AOIs")

    backup_aoi_list()

    old_ids = find_old_aoi_ids(existing)
    print(f"Old AOIs to replace: {len(old_ids)}")

    delete_old_data(old_ids)
    update_epochs(old_ids)
    clear_checkpoints()

    kept = strip_changed_events(existing)
    new_aois = run_step1_for_changed(kept)

    print("\nSorting by event order and reassigning IDs...")
    new_aois = sort_and_reindex(new_aois)

    with open(AOI_LIST, 'w') as f:
        json.dump(new_aois, f, indent=2)

    print(f"\nFinal aoi_list.json: {len(new_aois)} AOIs")
    print_xbd_summary(new_aois)
    print("\nDone. Next steps:")
    print("  python main.py run-step2a-query   # query epochs for new AOIs")
    print("  python main.py run-step2a-download # download scenes")
    print("  python main.py run-step2b          # co-register stacks")


if __name__ == '__main__':
    main()
