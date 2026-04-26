"""
autopilot_step3.py
------------------
Waits for step3a to finish, then runs step3b, QA, and pushes to GitHub.
Run alongside step3a (already started separately).
"""

import subprocess, sys, time
from pathlib import Path

LOG = Path("logs/autopilot_step3.log")
LOG.parent.mkdir(exist_ok=True)

def log(msg):
    line = f"[{time.strftime('%H:%M:%S')}] {msg}"
    print(line, flush=True)
    with open(LOG, "a") as f:
        f.write(line + "\n")

def count_damage_files():
    return len(list(Path("data/annotations").rglob("damage_polygons_T*.geojson")))

def expected_damage_files():
    import json
    aois = json.load(open("data/aoi_list.json"))
    total = 0
    for aoi in aois:
        import json as j
        meta_path = Path(f"data/stacks/{aoi['aoi_id']}_meta.json")
        if meta_path.exists():
            meta = j.load(open(meta_path))
            total += meta['n_epochs'] - 1
    return total

def run_cmd(label, args):
    log(f"Running: {label}...")
    r = subprocess.run([sys.executable] + args, capture_output=True, text=True)
    out = (r.stdout or '') + (r.stderr or '')
    log(out[-2000:] if out else "(no output)")
    log(f"{label} exit code: {r.returncode}")
    return r.returncode == 0

def push_github():
    log("Pushing to GitHub...")
    for cmd in [
        ["git", "add", "data/annotations/", "tdrd/pipelines/step3a_damage_annotation.py",
         "tdrd/pipelines/step3b_flood_extraction.py", "step3_qa.py",
         "scripts/download_sen1floods_labels.py", "scripts/autopilot_step3.py", "main.py"],
        ["git", "commit", "-m", "Step 3 complete: damage + flood annotations for 600 AOIs"],
        ["git", "push", "origin", "main"],
    ]:
        r = subprocess.run(cmd, capture_output=True, text=True)
        log(f"  {cmd[1]}: {(r.stdout + r.stderr).strip()[:400]}")

def main():
    log("=== autopilot_step3 started ===")
    expected = expected_damage_files()
    log(f"Expected damage files: {expected}")

    stalled = 0
    prev = count_damage_files()

    # Wait for step3a
    while True:
        current = count_damage_files()
        log(f"  Damage files: {current} / {expected}")
        if current >= expected:
            log("Step 3a complete.")
            break
        if current == prev:
            stalled += 1
        else:
            stalled = 0
        prev = current
        if stalled >= 6:  # 30 min no progress
            log(f"WARNING: No progress for 30 min. Proceeding with {current}/{expected}.")
            break
        time.sleep(300)

    run_cmd("step3b", ["main.py", "run-step3b"])
    run_cmd("step3-qa", ["step3_qa.py"])
    push_github()
    log("=== autopilot_step3 done ===")

if __name__ == "__main__":
    main()
