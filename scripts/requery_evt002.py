"""
Re-query epochs for the 52 failed EVT002 AOIs using relaxed cloud threshold (50%).
Updates aoi_epochs.json in-place for just those AOIs, then triggers re-download.
"""
import json, asyncio, os
import httpx
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from tdrd.config import STAC_API_URL, AOI_LIST_PATH
from tdrd.pipelines.step2a_query_epochs import select_epochs, _sub_days

EPOCHS_PATH = "data/aoi_epochs.json"
CLOUD_THRESHOLD = 50  # relaxed from 25

FAILED_IDS = {f"EVT002_{i:04d}" for i in range(68, 120)}


async def _requery_one(client, aoi, sem):
    bbox = aoi["bbox"]
    event_start, event_end = aoi["date_range"]
    pre_start = _sub_days(event_start, 30)
    datetime_str = f"{pre_start}T00:00:00Z/{event_end}T23:59:59Z"

    async with sem:
        try:
            s2_resp = await client.post(STAC_API_URL, json={
                "collections": ["sentinel-2-l2a"],
                "bbox":        bbox,
                "datetime":    datetime_str,
                "limit":       100,
                "query":       {"eo:cloud_cover": {"lt": CLOUD_THRESHOLD}},
            }, timeout=60.0)
            s2_items = s2_resp.json().get("features", []) if s2_resp.status_code == 200 else []

            s1_resp = await client.post(STAC_API_URL, json={
                "collections": ["sentinel-1-grd"],
                "bbox":        bbox,
                "datetime":    datetime_str,
                "limit":       100,
            }, timeout=60.0)
            s1_items = s1_resp.json().get("features", []) if s1_resp.status_code == 200 else []

            epochs = select_epochs(s2_items, s1_items, event_start, event_end)
            print(f"  {aoi['aoi_id']}: {len(epochs)} epochs (cloud<{CLOUD_THRESHOLD}%)", flush=True)
            return aoi["aoi_id"], epochs
        except Exception as e:
            print(f"  {aoi['aoi_id']}: ERROR {e}", flush=True)
            return aoi["aoi_id"], []


async def main():
    with open(AOI_LIST_PATH) as f:
        all_aois = json.load(f)
    failed_aois = [a for a in all_aois if a["aoi_id"] in FAILED_IDS]
    print(f"Re-querying {len(failed_aois)} EVT002 AOIs with cloud < {CLOUD_THRESHOLD}%...")

    with open(EPOCHS_PATH) as f:
        epochs_data = json.load(f)

    sem = asyncio.Semaphore(10)
    limits = httpx.Limits(max_connections=20, max_keepalive_connections=10)

    async with httpx.AsyncClient(limits=limits) as client:
        tasks = [_requery_one(client, aoi, sem) for aoi in failed_aois]
        for coro in asyncio.as_completed(tasks):
            aoi_id, epochs = await coro
            epochs_data[aoi_id] = epochs

    with open(EPOCHS_PATH, "w") as f:
        json.dump(epochs_data, f, indent=2)

    recovered = sum(1 for a in failed_aois if len(epochs_data[a["aoi_id"]]) >= 3)
    still_empty = sum(1 for a in failed_aois if len(epochs_data[a["aoi_id"]]) < 3)
    print(f"\nDone. Recovered: {recovered}/52  |  Still < 3 epochs: {still_empty}")


if __name__ == "__main__":
    if os.name == "nt":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())
