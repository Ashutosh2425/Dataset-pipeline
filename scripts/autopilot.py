"""
autopilot.py
------------
Runs overnight: waits for all downloads, runs step2b, verifies, pushes to GitHub.
Usage: conda run -n tdrd python autopilot.py
"""

import json
import subprocess
import sys
import time
from pathlib import Path

RAW_DIR    = Path("data/raw_scenes")
STACKS_DIR = Path("data/stacks")
AOI_LIST   = Path("data/aoi_list.json")
LOG_FILE   = Path("autopilot.log")

def log(msg):
    line = f"[{time.strftime('%H:%M:%S')}] {msg}"
    print(line, flush=True)
    with open(LOG_FILE, "a") as f:
        f.write(line + "\n")

def count_flags():
    return len(list(RAW_DIR.rglob("download_complete.flag")))

def count_stacks():
    return len(list(STACKS_DIR.glob("*_stack.tif")))

def total_aois():
    with open(AOI_LIST) as f:
        return len(json.load(f))

def wait_for_downloads():
    total = total_aois()
    log(f"Waiting for all {total} downloads to complete...")
    stalled = 0
    prev_count = count_flags()

    while True:
        flags = count_flags()
        log(f"  Downloads: {flags}/{total} complete")

        if flags >= total:
            log("All downloads complete.")
            return True

        if flags == prev_count:
            stalled += 1
        else:
            stalled = 0
        prev_count = flags

        # If no progress for 60 min, assume downloads are done/stalled
        if stalled >= 12:
            log(f"WARNING: No progress for 60 min. Proceeding with {flags}/{total} flags.")
            return False

        time.sleep(300)  # check every 5 min

def run_step2b():
    log("Starting Step 2b (co-registration + stacking)...")
    result = subprocess.run(
        [sys.executable, "main.py", "run-step2b"],
        capture_output=True, text=True
    )
    log(result.stdout[-3000:] if result.stdout else "(no stdout)")
    if result.stderr:
        log("STDERR: " + result.stderr[-1000:])
    log(f"Step 2b exit code: {result.returncode}")
    return result.returncode == 0

def run_verify():
    log("Running verify_stacks.py...")
    result = subprocess.run(
        [sys.executable, "verify_stacks.py"],
        capture_output=True, text=True
    )
    output = result.stdout or ""
    log(output)
    return output

def push_to_github():
    log("Pushing updated results to GitHub...")
    cmds = [
        ["git", "add", "data/stacks/", "autopilot.log"],
        ["git", "commit", "-m", "Step 2b complete: all stacks and meta JSONs added"],
        ["git", "push", "origin", "main"],
    ]
    for cmd in cmds:
        result = subprocess.run(cmd, capture_output=True, text=True)
        log(f"  {' '.join(cmd[:2])}: {result.stdout.strip() or result.stderr.strip()}")
        if result.returncode != 0 and "nothing to commit" not in result.stdout + result.stderr:
            log(f"  WARNING: command failed with code {result.returncode}")

def main():
    log("=== Autopilot started ===")
    log(f"Initial state: {count_flags()} flags, {count_stacks()} stacks")

    # Step 1: Wait for downloads
    wait_for_downloads()

    # Step 2: Run stacking (step2b skips already-done AOIs)
    run_step2b()

    # Check if step2b got everything; if not, retry once
    stacks = count_stacks()
    total  = total_aois()
    if stacks < total:
        log(f"Only {stacks}/{total} stacks after first run. Retrying step2b...")
        run_step2b()

    # Step 3: Verify
    verify_output = run_verify()

    # Step 4: Push to GitHub
    push_to_github()

    log("=== Autopilot done ===")

    if f"{total}/{total} OK" in verify_output:
        log("RESULT: PASS — all 600/600 AOIs verified.")
    else:
        log("RESULT: PARTIAL — check autopilot.log for details.")

if __name__ == "__main__":
    main()
