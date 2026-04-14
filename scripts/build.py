#!/usr/bin/env python3
"""Build SQLite database and static KV files from the raw XML data repo.

Supports incremental builds: pass --previous to reuse KV files and SQLite data
from a prior release, only importing new OS build folders and purging OS builds
that were removed from the repo.

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


def load_group_os_list(repo_root: Path, group: str) -> list[dict]:
    group_dir = repo_root / group
    list_path = group_dir / "list.json"

    if list_path.exists():
        with list_path.open() as f:
            return json.load(f)

    os_list = []
    for d in sorted(group_dir.iterdir()):
        meta = d / "meta.json"
        if meta.exists():
            with meta.open() as f:
                os_list.append(json.load(f))
    return os_list


def expected_os_keys(repo_root: Path, groups: list[str]) -> set[tuple[str, str, str]]:
    keys = set()

    for group in groups:
        group_dir = repo_root / group
        if not group_dir.exists():
            continue

        for os_info in load_group_os_list(repo_root, group):
            version_dir = group_dir / f"{os_info['version']}_{os_info['build']}"
            if not version_dir.exists():
                continue

            keys.add((os_info["name"], os_info["version"], os_info["build"]))

    return keys


def restore_previous_db(previous: Path, db_path: Path) -> bool:
    previous_db = previous / "ent.db"
    if not previous_db.exists():
        return False

    shutil.copy2(previous_db, db_path)
    return True


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

    db_path = output / "ent.db"

    groups_path = repo_root / "groups.json"
    if groups_path.exists():
        with groups_path.open() as f:
            groups = json.load(f)
    else:
        groups = [d.name for d in repo_root.iterdir() if d.is_dir() and not d.name.startswith(".")]

    existing = set()
    if args.previous:
        previous = Path(args.previous)
        if restore_previous_db(previous, db_path):
            print("Restored SQLite database from previous build")
        existing = find_existing_versions(previous)
        if existing:
            print(f"Found {len(existing)} existing versions from previous build")

    current_os_keys = expected_os_keys(repo_root, groups)

    print("=== Building SQLite database ===")
    imported_counts: dict[str, int] = {}
    for group in groups:
        group_dir = repo_root / group
        if not group_dir.exists():
            continue
        print(f"Importing group: {group}")
        imported_counts.update(import_data_repo(repo_root, str(db_path), group))

    reader = Reader(str(db_path))
    removed = reader.purge_missing_os(current_os_keys)
    for os_info in removed:
        print(
            f"Removed stale OS: {os_info['name']} {os_info['version']} {os_info['build']}"
        )

    print("\n=== Exporting static KV files ===")

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
            new_rows = imported_counts.get(version_key, 0)

            if version_key in existing and args.previous and new_rows == 0:
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
