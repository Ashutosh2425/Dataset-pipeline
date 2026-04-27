"""
validate_tdrd.py
----------------
Final validation script for the TDRD dataset.
Prints full stats confirming dataset meets spec targets.
"""

import json
from pathlib import Path
from collections import defaultdict

BASE = Path(".")
DATA = BASE / "data"

SPLIT_FILES = {
    "train":    DATA / "tdrd_train.jsonl",
    "val":      DATA / "tdrd_val.jsonl",
    "test":     DATA / "tdrd_test.jsonl",
    "conflict": DATA / "tdrd_conflict_subset.jsonl",
}

def load_jsonl(path):
    if not path.exists():
        return []
    return [json.loads(l) for l in open(path, encoding="utf-8")]


def check(label, value, condition, unit=""):
    icon = "[x]" if condition else "[ ]"
    val_str = f"{value}{(' ' + unit) if unit else ''}"
    print(f"  {icon} {label}: {val_str}")
    return condition


def main():
    print("=" * 55)
    print("  TDRD DATASET VALIDATION REPORT")
    print("=" * 55)

    all_ok = True

    # ── Load splits ────────────────────────────────────────────
    splits = {name: load_jsonl(path) for name, path in SPLIT_FILES.items()}
    total = len(splits["train"]) + len(splits["val"]) + len(splits["test"])

    print("\n[1] Split sizes")
    all_ok &= check("train",    len(splits["train"]),    len(splits["train"]) >= 1500)
    all_ok &= check("val",      len(splits["val"]),      len(splits["val"])   >= 300)
    all_ok &= check("test",     len(splits["test"]),     len(splits["test"])  >= 300)
    all_ok &= check("conflict_subset", len(splits["conflict"]), len(splits["conflict"]) >= 100)
    all_ok &= check("total verified", total,             total >= 2000)

    # ── Load raw queries ──────────────────────────────────────
    raw = load_jsonl(DATA / "queries_raw.jsonl")
    pass_rate = total / len(raw) * 100 if raw else 0

    print("\n[2] Verification")
    all_ok &= check("raw queries",  len(raw),    len(raw) >= 3000)
    all_ok &= check("pass rate",    f"{pass_rate:.1f}%", pass_rate >= 70)

    # ── Query type distribution (train+val+test combined) ─────
    all_verified = splits["train"] + splits["val"] + splits["test"]
    qt_counts = defaultdict(int)
    for q in all_verified:
        qt_counts[q["query_type"]] += 1

    print("\n[3] Query type distribution (train+val+test)")
    for qt in ["Q1_counting", "Q2_area", "Q3_proximity", "Q4_change", "Q5_routing", "Q6_conflict"]:
        count = qt_counts[qt]
        pct   = count / total * 100 if total else 0
        ok    = count >= 100
        all_ok &= ok
        icon = "[x]" if ok else "[ ]"
        print(f"  {icon} {qt:<18} {count:>5}  ({pct:.1f}%)")

    # ── Field completeness ────────────────────────────────────
    print("\n[4] Field completeness")
    required_fields = ["aoi_id", "event_id", "query_type", "query", "paraphrases",
                       "gt_answer", "gt_answer_text", "gt_tool_chain", "agents_required"]
    missing_counts = defaultdict(int)
    for q in all_verified:
        for f in required_fields:
            if not q.get(f):
                missing_counts[f] += 1

    for f in required_fields:
        mc = missing_counts[f]
        ok = mc == 0
        all_ok &= ok
        icon = "[x]" if ok else "[ ]"
        missing_str = f"  ({mc} missing)" if mc > 0 else ""
        print(f"  {icon} {f}{missing_str}")

    # ── gt_answer_text non-empty ──────────────────────────────
    empty_text = sum(1 for q in all_verified if not q.get("gt_answer_text", "").strip())
    print("\n[5] gt_answer_text populated")
    ok = empty_text == 0
    all_ok &= ok
    check("records with empty gt_answer_text", empty_text, ok)

    # ── AOI coverage ──────────────────────────────────────────
    aoi_list_path = DATA / "aoi_list.json"
    if aoi_list_path.exists():
        aoi_list = json.load(open(aoi_list_path))
        total_aois = len(aoi_list)
        covered_aois = {q["aoi_id"] for q in all_verified}
        coverage = len(covered_aois) / total_aois * 100 if total_aois else 0
        print("\n[6] AOI coverage")
        all_ok &= check("total AOIs", total_aois, total_aois == 600)
        all_ok &= check("AOIs with >= 1 verified query", len(covered_aois), len(covered_aois) >= 500)
        all_ok &= check("coverage %", f"{coverage:.1f}%", coverage >= 80)

    # ── Conflict events ────────────────────────────────────────
    conflict_path = DATA / "conflict_events.json"
    if conflict_path.exists():
        conflicts = json.load(open(conflict_path))
        print("\n[7] Conflict events")
        all_ok &= check("total conflict events", len(conflicts), len(conflicts) >= 300,
                        "(target ~468)")

    # ── Supporting files ────────────────────────────────────────
    print("\n[8] Required output files")
    required_files = [
        "data/tdrd_train.jsonl",
        "data/tdrd_val.jsonl",
        "data/tdrd_test.jsonl",
        "data/tdrd_conflict_subset.jsonl",
        "data/aoi_list.json",
        "data/conflict_events.json",
        "data/queries_raw.jsonl",
    ]
    for rp in required_files:
        exists = (BASE / rp).exists()
        all_ok &= exists
        icon = "[x]" if exists else "[ ]"
        print(f"  {icon} {rp}")

    # ── Summary ────────────────────────────────────────────────
    print("\n" + "=" * 55)
    if all_ok:
        print("  RESULT: ALL CHECKS PASSED — dataset is ready")
    else:
        print("  RESULT: SOME CHECKS FAILED — review above")
    print("=" * 55)

    print(f"\nDataset summary:")
    print(f"  train={len(splits['train'])}  val={len(splits['val'])}  "
          f"test={len(splits['test'])}  conflict_subset={len(splits['conflict'])}")
    print(f"  Total verified: {total}  |  Raw queries: {len(raw)}  |  "
          f"Pass rate: {pass_rate:.1f}%")
    print(f"  Query types: {dict(sorted(qt_counts.items()))}")


if __name__ == "__main__":
    main()
