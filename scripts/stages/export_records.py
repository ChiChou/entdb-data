"""Stage: export the SQLite database as precomputed JSON records.

Every frontend query is answered by fetching one small, whole, gzip-friendly
JSON file instead of many HTTP range reads. Entitlements barely change between
builds (85-89% of binaries are identical to the previous build), so records
store build *ranges* rather than repeating data per build.

Layout (per group, e.g. iOS/):

    groups.json                      ["iOS", "mac", "osx"]
    {group}/list.json                builds sorted by version; a build's index
                                     in this list is its *ordinal*
    {group}/b/{version}_{build}.json {"paths": [...], "keys": [[key, count], ...]}
    {group}/k/{hash(key)}.json       {"key", "values": [...],
                                      "rows": [[path, valueIndex, ranges], ...]}
    {group}/p/{hash(path)}.json      {"path", "plists": [xml, ...],
                                      "runs": [[from, to, plistIndex], ...]}

- ``ranges`` is a flat list of inclusive ordinal ranges ``[from, to, from, to,
  ...]``; ``runs`` are inclusive ordinal ranges for which the binary had the
  given plist.
- A key record answers "which binaries have this key" for every build, plus
  each binary's value (values are deduplicated per key, as canonical JSON
  with sorted object keys).
- A path record answers "this binary's entitlements" for every build (the
  original XML, deduplicated), which also gives its version history.
- ``hash`` is 64-bit FNV-1a over the UTF-8 name, as 16 hex digits. It is
  cheap to compute synchronously in the browser; collisions are checked here.
"""

import hashlib
import json
import re
import sqlite3
from collections import defaultdict
from pathlib import Path

FNV_OFFSET = 0xCBF29CE484222325
FNV_PRIME = 0x100000001B3
MASK64 = 0xFFFFFFFFFFFFFFFF

# Some blobs carry trailing data after the plist (e.g. "</plist>{...}").
TRAILER = re.compile(r"</plist>\s*{", re.IGNORECASE)


def fnv1a64(text: str) -> str:
    h = FNV_OFFSET
    for byte in text.encode("utf-8"):
        h ^= byte
        h = (h * FNV_PRIME) & MASK64
    return f"{h:016x}"


def version_key(os_info: dict) -> tuple:
    parts = tuple(int(p) if p.isdigit() else 0 for p in os_info["version"].split("."))
    return parts, os_info["build"]


def clean_xml(xml: str | bytes) -> str:
    # The bin.xml column holds raw bytes from the imported files.
    if isinstance(xml, bytes):
        xml = xml.decode("utf-8", errors="replace")
    match = TRAILER.search(xml)
    return xml[: match.start() + len("</plist>")] if match else xml


def to_ranges(ordinals: list[int]) -> list[int]:
    """Sorted ordinals -> flat inclusive ranges [from, to, from, to, ...]."""
    out: list[int] = []
    for i in sorted(ordinals):
        if out and out[-1] == i - 1:
            out[-1] = i
        else:
            out += [i, i]
    return out


def dump(obj) -> str:
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"))


class HashedDir:
    """Writes {dir}/{hash(name)}.json and fails on hash collisions."""

    def __init__(self, path: Path):
        self.path = path
        self.path.mkdir(parents=True, exist_ok=True)
        self.names: dict[str, str] = {}

    def write(self, name: str, obj) -> None:
        h = fnv1a64(name)
        other = self.names.setdefault(h, name)
        if other != name:
            raise RuntimeError(f"hash collision in {self.path}: {other!r} vs {name!r}")
        (self.path / f"{h}.json").write_text(dump(obj), encoding="utf-8")


def export_group(conn: sqlite3.Connection, os_list: list[dict], group_out: Path) -> dict:
    """Write list.json and all records for one group. os_list items need "id"."""
    builds = sorted(os_list, key=version_key)
    group_out.mkdir(parents=True, exist_ok=True)

    public = [{k: v for k, v in o.items() if k != "id"} for o in builds]
    (group_out / "list.json").write_text(dump(public), encoding="utf-8")

    path_ids: dict[str, int] = {}
    paths: list[str] = []

    def path_id(p: str) -> int:
        pid = path_ids.get(p)
        if pid is None:
            pid = path_ids[p] = len(paths)
            paths.append(p)
        return pid

    # path id -> xml id -> [ordinals]; xml texts are deduplicated by digest.
    path_plists: dict[int, dict[int, list[int]]] = defaultdict(lambda: defaultdict(list))
    xml_ids: dict[bytes, int] = {}
    xmls: list[str] = []
    # key -> (path id, canonical value) -> [ordinals]
    key_rows: dict[str, dict[tuple[int, str], list[int]]] = defaultdict(lambda: defaultdict(list))

    build_dir = group_out / "b"
    build_dir.mkdir(parents=True, exist_ok=True)

    for ordinal, os_info in enumerate(builds):
        osid = os_info["id"]
        build_paths = []
        for p, raw in conn.execute("SELECT path, xml FROM bin WHERE osid=?", (osid,)):
            xml = clean_xml(raw)
            if "\ufffd" in xml and (isinstance(raw, str) or b"\xef\xbf\xbd" not in raw):
                print(f"  warning: invalid UTF-8 replaced in {os_info['version']} {p}")
            digest = hashlib.sha1(xml.encode("utf-8")).digest()
            xid = xml_ids.get(digest)
            if xid is None:
                xid = xml_ids[digest] = len(xmls)
                xmls.append(xml)
            path_plists[path_id(p)][xid].append(ordinal)
            build_paths.append(p)

        key_counts: dict[str, int] = defaultdict(int)
        cursor = conn.execute(
            "SELECT bin.path, pair.key, pair.value FROM pair "
            "JOIN bin ON pair.binid = bin.id WHERE bin.osid=?",
            (osid,),
        )
        for p, key, value in cursor:
            canon = json.dumps(json.loads(value), sort_keys=True, separators=(",", ":"), ensure_ascii=False)
            key_rows[key][(path_id(p), canon)].append(ordinal)
            key_counts[key] += 1

        tag = f"{os_info['version']}_{os_info['build']}"
        (build_dir / f"{tag}.json").write_text(
            dump({"paths": sorted(build_paths), "keys": sorted(key_counts.items())}),
            encoding="utf-8",
        )

    key_dir = HashedDir(group_out / "k")
    for key, rows in key_rows.items():
        values: dict[str, int] = {}
        out_rows = []
        for (pid, canon), ordinals in sorted(rows.items(), key=lambda r: (paths[r[0][0]], r[0][1])):
            vi = values.setdefault(canon, len(values))
            out_rows.append([paths[pid], vi, to_ranges(ordinals)])
        key_dir.write(key, {"key": key, "values": [json.loads(v) for v in values], "rows": out_rows})

    path_dir = HashedDir(group_out / "p")
    for pid, by_xml in path_plists.items():
        plists = []
        runs = []
        for xid, ordinals in by_xml.items():
            index = len(plists)
            plists.append(xmls[xid])
            ranges = to_ranges(ordinals)
            runs += [[ranges[i], ranges[i + 1], index] for i in range(0, len(ranges), 2)]
        runs.sort()
        path_dir.write(paths[pid], {"path": paths[pid], "plists": plists, "runs": runs})

    return {"builds": len(builds), "keys": len(key_rows), "paths": len(path_plists), "plists": len(xmls)}


def export_records(db_path: str, group_os: dict[str, list[dict]], output: Path) -> dict:
    """Export every group. ``group_os`` maps group -> OS dicts (with "id")."""
    output.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    stats = {}
    for group, os_list in group_os.items():
        if os_list:
            stats[group] = export_group(conn, os_list, output / group)
    (output / "groups.json").write_text(dump(list(group_os)), encoding="utf-8")
    return stats


def main():
    import argparse

    from indexer.db import Reader

    parser = argparse.ArgumentParser(description="Export the database as JSON records")
    parser.add_argument("db", help="Path to SQLite database")
    parser.add_argument("output", help="Output directory")
    args = parser.parse_args()

    # Without the data repo at hand, put every OS in one group named "all".
    stats = export_records(args.db, {"all": Reader(args.db).all_os()}, Path(args.output))
    print(json.dumps(stats))


if __name__ == "__main__":
    main()
