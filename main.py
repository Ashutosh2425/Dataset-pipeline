"""
main.py
-------
Professional CLI entry point for the TDRD Dataset Generation Tool.
Usage:
    python main.py check-aois
    python main.py run-step1
"""

import sys
import argparse

# Add tdrd to sys.path if needed
from tdrd.pipelines.step1_select_aois import Step1Pipeline
from tdrd.pipelines.step2a_query_epochs import QueryEpochsPipeline
from tdrd.pipelines.step2a_download_scenes import Step2aPipeline
from tdrd.pipelines.step2b_coregister import Step2bPipeline
from tdrd.pipelines.step3_analyze_aois import Step3PrepPipeline

def cli_check_aois(args):
    """Verifies existing AOI list against build guide criteria."""
    pipeline = Step1Pipeline()
    if pipeline.verify_existing():
        print("[SUCCESS] AOI list is complete and verified.")
    else:
        print("[FAIL] AOI list does not meet criteria. Run run-step1 to fix.")

def cli_run_step1(args):
    """Executes Step 1 (AOI Selection) from scratch or resumes progress."""
    pipeline = Step1Pipeline()
    pipeline.run()

def cli_run_step2a_query(args):
    """Step 2A: Query Element84 STAC for 3-5 epochs per AOI."""
    pipeline = QueryEpochsPipeline()
    pipeline.run()

def cli_run_step2a(args):
    """Executes Step 2a (Download Scenes) logic."""
    pipeline = Step2aPipeline()
    pipeline.run()

def cli_run_step2b(args):
    """Executes Step 2b (Co-registration) logic."""
    pipeline = Step2bPipeline()
    pipeline.run()

def cli_run_step3_prep(args):
    """Executes Step 3 Prep (Analysis of AOIs) logic."""
    pipeline = Step3PrepPipeline()
    pipeline.run()

def main():
    parser = argparse.ArgumentParser(description="TDRD Dataset Generation CLI")
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # Step 1 Commands
    subparsers.add_parser("check-aois",  help="Verify existing aoi_list.json")
    subparsers.add_parser("run-step1",   help="Compute/Resume AOI selection")

    # Step 2 Commands
    subparsers.add_parser("run-step2a-query",  help="Query Element84 for 3-5 epochs per AOI")
    subparsers.add_parser("run-step2a-download", help="Download windowed COG crops per epoch")
    subparsers.add_parser("run-step2b",  help="Co-register epochs into temporal stacks")

    # Step 3 Prep Commands
    subparsers.add_parser("run-step3-prep", help="Analyze AOI demographics/buildings")

    args = parser.parse_args()

    if args.command == "check-aois":
        cli_check_aois(args)
    elif args.command == "run-step1":
        cli_run_step1(args)
    elif args.command == "run-step2a-query":
        cli_run_step2a_query(args)
    elif args.command == "run-step2a-download":
        cli_run_step2a(args)
    elif args.command == "run-step2b":
        cli_run_step2b(args)
    elif args.command == "run-step3-prep":
        cli_run_step3_prep(args)
    else:
        parser.print_help()
        sys.exit(1)

if __name__ == "__main__":
    main()
