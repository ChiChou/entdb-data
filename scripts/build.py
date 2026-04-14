#!/usr/bin/env python3
"""Build SQLite database and static KV files from the raw XML data repo.

Supports incremental builds: pass --previous to reuse KV files from a prior
release, only rebuilding versions that are new or missing.

Produces:
    output/
        ent.db              # Merged SQLite database (for WASM engine)
        {group}/
            list.json
            {version}_{build}/
                paths.txt
                meta.json
                blobs.index.json + blobs.txt
                keys.index.json + keys.txt
        groups.json
"""

import json
import sys
import shutil
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from stages.db_import import import_data_repo
from indexer.db import Reader
from indexer.kv import KVStore


def find_existing_versions(previous: Path) -> set[str]:
    existing = set()
    groups_path = previous / "groups.json"
    if not groups_path.exists():
        return existing

    with groups_path.open() as f:
        groups = json.load(f)

    for group in groups:
        group_dir = previous / group
        if not group_dir.exists():
            continue
        for d in group_dir.iterdir():
            if d.is_dir() and (d / "blobs.index.json").exists():
                existing.add(f"{group}/{d.name}")
    return existing


def build_kv_for_version(reader: Reader, osid: int, subdir: Path):
    subdir.mkdir(parents=True, exist_ok=True)

    with (subdir / "paths.txt").open("w") as fp:
        fp.write("\n".join(reader.paths_by_osid(osid)))

    with KVStore(subdir / "blobs.index.json", subdir / "blobs.txt") as blobs_store:
        for b in reader.binaries_by_osid(osid):
            blobs_store.add(b["path"], b["xml"])

    with KVStore(subdir / "keys.index.json", subdir / "keys.txt") as keys_store:
        for key in reader.keys_by_osid(osid):
            paths = reader.owns_key_by_osid(osid, key)
            keys_store.add(key, "\n".join(paths).encode())


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Build data artifacts from raw XML repo")
    parser.add_argument("-o", "--output", required=True, help="Output directory")
    parser.add_argument(
        "--previous", help="Path to previous build output (for incremental builds)",
    )
    parser.add_argument(
        "--repo-root", default=str(Path(__file__).parent.parent),
        help="Path to data repo root (default: parent of scripts/)",
    )
    args = parser.parse_args()

    repo_root = Path(args.repo_root)
    output = Path(args.output)
    output.mkdir(parents=True, exist_ok=True)

    db_path = str(output / "ent.db")

    groups_path = repo_root / "groups.json"
    if groups_path.exists():
        with groups_path.open() as f:
            groups = json.load(f)
    else:
        groups = [d.name for d in repo_root.iterdir() if d.is_dir() and not d.name.startswith(".")]

    existing = set()
    if args.previous:
        previous = Path(args.previous)
        existing = find_existing_versions(previous)
        if existing:
            print(f"Found {len(existing)} existing versions from previous build")

    print("=== Building SQLite database ===")
    for group in groups:
        group_dir = repo_root / group
        if not group_dir.exists():
            continue
        print(f"Importing group: {group}")
        import_data_repo(repo_root, db_path, group)

    print("\n=== Exporting static KV files ===")
    reader = Reader(db_path)

    all_os = reader.all_os()
    group_builds: dict[str, list[dict]] = {g: [] for g in groups}

    for os_info in all_os:
        build = os_info["build"]
        version = os_info["version"]
        tag = f"{version}_{build}"
        for g in groups:
            if (repo_root / g / tag).exists():
                group_builds[g].append(os_info)
                break

    reused = 0
    built = 0

    for group, os_list in group_builds.items():
        if not os_list:
            continue
        group_out = output / group
        group_out.mkdir(parents=True, exist_ok=True)

        for os_info in os_list:
            build = os_info["build"]
            osid = os_info["id"]
            tag = f"{os_info['version']}_{build}"
            version_key = f"{group}/{tag}"
            subdir = group_out / tag

            if version_key in existing and args.previous:
                src = Path(args.previous) / group / tag
                if src.exists():
                    shutil.copytree(src, subdir, dirs_exist_ok=True)
                    with open(subdir / "meta.json", "w") as fp:
                        json.dump(os_info, fp)
                    reused += 1
                    continue

            build_kv_for_version(reader, osid, subdir)
            with open(subdir / "meta.json", "w") as fp:
                json.dump(os_info, fp)
            built += 1
            print(f"  {version_key}")

        with (group_out / "list.json").open("w") as fp:
            json.dump(os_list, fp)

    with (output / "groups.json").open("w") as fp:
        json.dump(groups, fp)

    print(f"\nDone. Reused: {reused}, Built: {built}, Total: {reused + built}")
    print(f"Output: {output}")


if __name__ == "__main__":
    main()
