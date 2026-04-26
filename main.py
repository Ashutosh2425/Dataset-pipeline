"""
main.py
-------
CLI entry point for the TDRD Dataset Generation Tool.
Usage:
    python main.py check-aois
    python main.py run-step1
    python main.py run-step3a --force-xbd   # after loading xBD data
    python main.py re-run-after-xbd          # full chain for xBD events
"""

import sys
import argparse

from tdrd.pipelines.step1_select_aois import Step1Pipeline
from tdrd.pipelines.step2a_query_epochs import QueryEpochsPipeline
from tdrd.pipelines.step2a_download_scenes import Step2aPipeline
from tdrd.pipelines.step2b_coregister import Step2bPipeline
from tdrd.pipelines.step3_analyze_aois import Step3PrepPipeline
from tdrd.pipelines.step3a_damage_annotation import Step3aDamagePipeline, discover_xbd_event_map
from tdrd.pipelines.step3b_flood_extraction import Step3bFloodPipeline
from tdrd.pipelines.step4_road_damage_scoring import Step4RoadDamagePipeline
from tdrd.pipelines.step5b_compute_routes import Step5EvacuationPipeline
from tdrd.pipelines.step6_conflict_annotation import Step6ConflictPipeline
from tdrd.pipelines.step7_query_generation import Step7QueryPipeline


def cli_check_aois(args):
    pipeline = Step1Pipeline()
    if pipeline.verify_existing():
        print("[SUCCESS] AOI list is complete and verified.")
    else:
        print("[FAIL] AOI list does not meet criteria. Run run-step1 to fix.")

def cli_run_step1(args):
    Step1Pipeline().run()

def cli_run_step2a_query(args):
    QueryEpochsPipeline().run()

def cli_run_step2a(args):
    Step2aPipeline().run()

def cli_run_step2b(args):
    Step2bPipeline().run()

def cli_run_step3_prep(args):
    Step3PrepPipeline().run()

def cli_run_step3a(args):
    Step3aDamagePipeline().run(force_xbd=getattr(args, 'force_xbd', False))

def cli_run_step3b(args):
    Step3bFloodPipeline().run()

def cli_run_step4(args):
    workers = getattr(args, 'workers', 2)
    Step4RoadDamagePipeline(workers=workers).run(force=getattr(args, 'force', False))

def cli_run_step5(args):
    workers = getattr(args, 'workers', 2)
    Step5EvacuationPipeline(workers=workers).run(force=getattr(args, 'force', False))

def cli_run_step6(args):
    workers = getattr(args, 'workers', 4)
    Step6ConflictPipeline(workers=workers).run()

def cli_run_step7(args):
    workers = getattr(args, 'workers', 2)
    Step7QueryPipeline(workers=workers).run()

def cli_rerun_after_xbd(args):
    """
    After placing xBD data into data/xbd/, run this command to:
      1. Re-annotate damage polygons for all xBD events (from real building footprints)
      2. Re-score road damage for xBD event AOIs (force overwrite)
      3. Re-compute evacuation routes for xBD event AOIs (force overwrite)
      4. Run conflict annotation across all AOIs

    Non-xBD events are not touched — their spectral outputs remain valid.
    """
    xbd_map = discover_xbd_event_map()
    if not xbd_map:
        print("ERROR: No xBD data found in data/xbd/. Load xBD first.")
        print("  Expected structure: data/xbd/{disaster-name}/labels/*.json")
        sys.exit(1)

    xbd_event_ids = set(xbd_map.keys())
    print(f"Re-running pipeline for xBD events: {sorted(xbd_event_ids)}")

    # Step 3a: delete existing xBD event outputs and re-annotate
    print("\n[1/4] Step 3a — Re-annotating xBD events...")
    Step3aDamagePipeline().run(force_xbd=True)

    # Step 4: force re-score for xBD event AOIs
    import json
    from pathlib import Path
    aoi_list = json.load(open('data/aoi_list.json'))
    xbd_aoi_ids = {a['aoi_id'] for a in aoi_list if a['event_id'] in xbd_event_ids}

    print(f"\n[2/4] Step 4 — Re-scoring {len(xbd_aoi_ids)} xBD AOIs...")
    p4 = Step4RoadDamagePipeline(workers=getattr(args, 'workers', 2))
    # Force-delete GPKGs for xBD AOIs so they get re-scored
    for aoi_id in xbd_aoi_ids:
        gpkg = Path('data/annotations') / aoi_id / 'road_damage_scores.gpkg'
        if gpkg.exists():
            gpkg.unlink()
    p4.run(force=False)   # force=False: processes all AOIs with missing GPKG (which we just deleted)

    # Step 5: re-compute routes for xBD AOIs
    print(f"\n[3/4] Step 5 — Re-computing routes for {len(xbd_aoi_ids)} xBD AOIs...")
    Step5EvacuationPipeline(workers=getattr(args, 'workers', 2)).run(
        force=False, event_ids=xbd_event_ids
    )

    # Step 6: conflict annotation (always full run)
    print("\n[4/4] Step 6 — Conflict annotation (all AOIs)...")
    Step6ConflictPipeline(workers=getattr(args, 'workers', 4)).run()

    print("\nRe-run complete.")


def main():
    parser = argparse.ArgumentParser(description="TDRD Dataset Generation CLI")
    sub = parser.add_subparsers(dest="command", help="Available commands")

    sub.add_parser("check-aois",  help="Verify existing aoi_list.json")
    sub.add_parser("run-step1",   help="Compute/Resume AOI selection")

    sub.add_parser("run-step2a-query",    help="Query Element84 for 3-5 epochs per AOI")
    sub.add_parser("run-step2a-download", help="Download windowed COG crops per epoch")
    sub.add_parser("run-step2b",          help="Co-register epochs into temporal stacks")

    sub.add_parser("run-step3-prep", help="Analyze AOI demographics/buildings")

    p3a = sub.add_parser("run-step3a", help="Transfer building damage labels (xBD + spectral fallback)")
    p3a.add_argument("--force-xbd", action="store_true",
                     help="Delete and re-annotate damage polygons for xBD events from real building data")

    sub.add_parser("run-step3b", help="Extract flood extent (Sen1Floods11 GT + NDWI fallback)")

    p4 = sub.add_parser("run-step4", help="Score road damage per AOI per epoch")
    p4.add_argument("--workers", type=int, default=2)
    p4.add_argument("--force", action="store_true", help="Re-score all AOIs even if GPKG exists")

    p5 = sub.add_parser("run-step5", help="Compute evacuation route ground truth")
    p5.add_argument("--workers", type=int, default=2)
    p5.add_argument("--force", action="store_true", help="Re-compute routes for all AOIs")

    p6 = sub.add_parser("run-step6", help="Detect conflict episodes")
    p6.add_argument("--workers", type=int, default=4)

    p7 = sub.add_parser("run-step7", help="Generate NL queries with Ollama")
    p7.add_argument("--workers", type=int, default=2)

    pxbd = sub.add_parser(
        "re-run-after-xbd",
        help="After loading xBD data: re-annotate + re-score + re-route + detect conflicts"
    )
    pxbd.add_argument("--workers", type=int, default=2)

    args = parser.parse_args()

    dispatch = {
        "check-aois":        cli_check_aois,
        "run-step1":         cli_run_step1,
        "run-step2a-query":  cli_run_step2a_query,
        "run-step2a-download": cli_run_step2a,
        "run-step2b":        cli_run_step2b,
        "run-step3-prep":    cli_run_step3_prep,
        "run-step3a":        cli_run_step3a,
        "run-step3b":        cli_run_step3b,
        "run-step4":         cli_run_step4,
        "run-step5":         cli_run_step5,
        "run-step6":         cli_run_step6,
        "run-step7":         cli_run_step7,
        "re-run-after-xbd":  cli_rerun_after_xbd,
    }

    fn = dispatch.get(args.command)
    if fn:
        fn(args)
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
