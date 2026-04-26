"""
Extract xBD label JSON files from all archive parts into data/xbd/.

Handles split archives (part-aa, part-ab, ...) by chaining them into
one stream. Skips all GeoTIFF images — only extracts label JSONs.

Archive structure:
  geotiffs/{split}/labels/{event}_{id}_{pre|post}_disaster.json

Target structure:
  data/xbd/{event}/labels/{event}_{id}_{pre|post}_disaster.json

Run from repo root:
    python scripts/extract_xbd_labels.py
"""

import re
import io
import tarfile
from pathlib import Path
from collections import defaultdict

DATASET_DIR = Path("dataset")
XBD_DIR     = Path("data/xbd")
XBD_DIR.mkdir(parents=True, exist_ok=True)


# ── Chained file reader for split archives ────────────────────────────────────

class ChainedFiles(io.RawIOBase):
    """
    Presents multiple files as one continuous byte stream.
    Used to read split archives (part-aa + part-ab + ...) as a single file.
    """
    def __init__(self, paths):
        self.paths   = list(paths)
        self.index   = 0
        self.current = open(self.paths[0], "rb") if self.paths else None

    def read(self, n=-1):
        if self.current is None:
            return b""
        buf = b""
        while True:
            chunk = self.current.read(n - len(buf) if n != -1 else -1)
            buf += chunk
            if n != -1 and len(buf) >= n:
                break
            if chunk == b"":
                self.current.close()
                self.index += 1
                if self.index >= len(self.paths):
                    self.current = None
                    break
                self.current = open(self.paths[self.index], "rb")
        return buf

    def readinto(self, b):
        data = self.read(len(b))
        n = len(data)
        b[:n] = data
        return n

    def readable(self):
        return True

    def close(self):
        if self.current:
            self.current.close()
        super().close()


# ── Archive detection ─────────────────────────────────────────────────────────

def find_archive_groups():
    """
    Scan dataset/ and return list of archive groups.
    Each group is a list of Path objects that together form one archive,
    ordered correctly (part-aa before part-ab, etc.).

    Returns: list of (label, [path, ...]) tuples.
    """
    all_files = sorted(DATASET_DIR.iterdir())
    groups    = {}

    for f in all_files:
        if not f.is_file():
            continue
        name = f.name

        # Split archive: name.part-aa, name.part-ab, ...
        m = re.match(r"^(.+\.tgz|.+\.tar\.gz|.+\.tar)\.part-([a-z]+)$", name)
        if m:
            base = m.group(1)
            groups.setdefault(base, []).append(f)
            continue

        # Single archive
        if any(name.endswith(ext) for ext in (".tgz", ".tar.gz", ".tar", ".tar.bz2")):
            groups.setdefault(name, [f])

    # Sort parts within each group
    result = []
    for base, files in groups.items():
        files.sort(key=lambda p: p.name)
        result.append((base, files))

    return sorted(result)


# ── Label extraction ──────────────────────────────────────────────────────────

def event_from_filename(filename):
    m = re.match(r"^(.+?)_(\d+)_(pre|post)_disaster\.json$", filename)
    return m.group(1) if m else None


def extract_group(label, parts, already_on_disk):
    """
    Stream through one archive (single file or chained parts) and
    extract label JSONs that are not already on disk.
    Returns {event: count_newly_extracted}.
    """
    total_size = sum(p.stat().st_size for p in parts)
    print(f"\n  [{label}]  {len(parts)} part(s)  "
          f"({total_size / 1e9:.1f} GB total)")

    if len(parts) == 1:
        raw = open(parts[0], "rb")
    else:
        raw = ChainedFiles(parts)

    buf = io.BufferedReader(raw, buffer_size=1 << 20)

    # Detect compression
    name = label.lower()
    if name.endswith(".tgz") or name.endswith(".tar.gz"):
        mode = "r|gz"
    elif name.endswith(".tar.bz2"):
        mode = "r|bz2"
    else:
        mode = "r|"

    try:
        tf = tarfile.open(fileobj=buf, mode=mode)
    except Exception as e:
        print(f"    Cannot open archive: {e}")
        buf.close()
        return {}

    extracted     = defaultdict(int)
    skipped_exist = 0
    skipped_tifs  = 0
    errors        = 0
    i             = 0

    try:
        for member in tf:
            i += 1
            if not member.isfile():
                continue

            path = member.name
            if not path.endswith(".json"):
                if path.endswith(".tif"):
                    skipped_tifs += 1
                continue

            filename = Path(path).name
            event    = event_from_filename(filename)
            if event is None:
                continue

            dest_dir  = XBD_DIR / event / "labels"
            dest_dir.mkdir(parents=True, exist_ok=True)
            dest_file = dest_dir / filename

            if dest_file.exists():
                skipped_exist += 1
                continue

            try:
                fobj = tf.extractfile(member)
                if fobj is not None:
                    dest_file.write_bytes(fobj.read())
                    extracted[event] += 1
            except Exception as e:
                errors += 1
                if errors <= 3:
                    print(f"    Error: {filename}: {e}")

            if i % 2000 == 0:
                new = sum(extracted.values())
                print(f"    {i:,} members | +{new} new JSONs | "
                      f"{skipped_exist} existed | {skipped_tifs} TIFs skipped")

    except (tarfile.ReadError, EOFError) as e:
        print(f"    Archive ended: {e}")
    finally:
        try:
            tf.close()
        except Exception:
            pass
        buf.close()

    new_total = sum(extracted.values())
    print(f"    Done: +{new_total} new  |  {skipped_exist} existed  "
          f"|  {skipped_tifs} TIFs skipped  |  {errors} errors")
    return dict(extracted)


# ── Pipeline coverage check ───────────────────────────────────────────────────

def show_pipeline_coverage(all_events):
    event_keywords = {
        "EVT001": ["harvey"],
        "EVT003": ["camp-fire", "camp_fire", "campfire", "paradise",
                   "california", "santa-rosa", "socal"],
        "EVT005": ["midwest"],
        "EVT006": ["beirut"],
        "EVT008": ["natchez", "tuscaloosa", "joplin", "tornado"],
        "EVT010": ["turkey", "kahramanmaras", "earthquake"],
        "EVT011": ["hawaii", "maui"],
    }
    print("\n  Pipeline event coverage:")
    for eid, kws in event_keywords.items():
        matches = [ev for ev in all_events if any(kw in ev.lower() for kw in kws)]
        if matches:
            total = sum(all_events[m] for m in matches)
            print(f"    {eid}: COVERED  ({', '.join(matches)})  {total:,} files")
        else:
            print(f"    {eid}: not found in any archive")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    groups = find_archive_groups()
    if not groups:
        print(f"No archives found in {DATASET_DIR}/")
        return

    print(f"Found {len(groups)} archive group(s) in {DATASET_DIR}/:")
    for label, parts in groups:
        size = sum(p.stat().st_size for p in parts)
        print(f"  {label}  ({len(parts)} part(s), {size/1e9:.1f} GB)")

    # Count what's already on disk
    on_disk = defaultdict(int)
    if XBD_DIR.exists():
        for ev_dir in XBD_DIR.iterdir():
            lbl = ev_dir / "labels"
            if lbl.exists():
                on_disk[ev_dir.name] = sum(1 for _ in lbl.glob("*.json"))
    if on_disk:
        print(f"\nAlready on disk: {sum(on_disk.values()):,} files "
              f"across {len(on_disk)} events")

    # Extract from each group
    all_events = defaultdict(int)
    for ev, cnt in on_disk.items():
        all_events[ev] += cnt

    print(f"\nExtracting label JSONs (skipping all GeoTIFFs)...")
    for label, parts in groups:
        new = extract_group(label, parts, set())
        for ev, cnt in new.items():
            all_events[ev] += cnt

    # Summary
    print(f"\n{'='*55}")
    print(f"All events in data/xbd/:")
    for ev in sorted(all_events):
        print(f"  {ev:<40} {all_events[ev]:>5} files")
    print(f"  {'TOTAL':<40} {sum(all_events.values()):>5}")

    show_pipeline_coverage(dict(all_events))

    print(f"\nNext: python scripts/run_xbd_pipeline.py")


if __name__ == "__main__":
    main()
