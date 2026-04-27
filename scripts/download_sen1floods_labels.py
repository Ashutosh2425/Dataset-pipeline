"""
Download all Sen1Floods11 hand-labeled TIFs from GCS.
Output: data/sen1floods11/v1.1/data/LabelHand/{chip_id}_LabelHand.tif
"""

import json
from pathlib import Path
import httpx
from tqdm import tqdm

CATALOG_DIR = Path("data/sen1floods11/v1.1/catalog/sen1floods11_hand_labeled_label")
OUT_DIR     = Path("data/sen1floods11/v1.1/data/LabelHand")
OUT_DIR.mkdir(parents=True, exist_ok=True)

def main():
    chips = []
    for chip_dir in CATALOG_DIR.iterdir():
        if not chip_dir.is_dir():
            continue
        for jf in chip_dir.glob("*.json"):
            try:
                d = json.load(open(jf))
                url = d.get('assets', {}).get('LabelHand', {}).get('href')
                chip_id = d.get('id', '')
                bbox = d.get('bbox')
                if url and chip_id and bbox:
                    chips.append({'id': chip_id, 'url': url, 'bbox': bbox})
            except Exception:
                continue

    print(f"Found {len(chips)} chips to download → {OUT_DIR}")

    ok = skipped = failed = 0
    with httpx.Client(timeout=30) as client:
        for chip in tqdm(chips, desc="Downloading labels"):
            fname = chip['url'].split('/')[-1]
            out   = OUT_DIR / fname
            if out.exists():
                skipped += 1
                continue
            try:
                r = client.get(chip['url'])
                if r.status_code == 200:
                    out.write_bytes(r.content)
                    ok += 1
                else:
                    failed += 1
            except Exception as e:
                print(f"  FAIL {fname}: {e}")
                failed += 1

    # Save index with bbox for spatial lookup
    index = [{'id': c['id'], 'bbox': c['bbox'],
               'path': str(OUT_DIR / c['url'].split('/')[-1])} for c in chips]
    with open("data/sen1floods11/label_index.json", 'w') as f:
        json.dump(index, f, indent=2)

    print(f"\nDone: {ok} downloaded, {skipped} skipped, {failed} failed")
    print(f"Index saved: data/sen1floods11/label_index.json")

if __name__ == "__main__":
    main()
