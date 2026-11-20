#!/usr/bin/env python3
"""Build the SQLite database and the static JSON records from the raw XML repo.

Supports incremental builds: pass --previous with a directory holding a prior
release's ent.db; only new OS build folders are imported, and OS builds that
were removed from the repo are purged. The records are regenerated from the
database every time (they store build ranges, so a new build touches many of
them anyway).

Produces:
    output/
        ent.db              # Merged SQLite database (release asset, not served)
        groups.json
        {group}/
            list.json       # builds sorted by version (index = ordinal)
            b/{version}_{build}.json
            k/{hash(key)}.json
            p/{hash(path)}.json

See stages/export_records.py for the record formats.
"""

import json
import sys
import shutil
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from stages.db_import import import_data_repo
from stages.export_records import export_records
from indexer.db import Reader


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


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Build data artifacts from raw XML repo")
    parser.add_argument("-o", "--output", required=True, help="Output directory")
    parser.add_argument(
        "--previous", help="Directory with a previous ent.db (for incremental builds)",
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

    if args.previous and restore_previous_db(Path(args.previous), db_path):
        print("Restored SQLite database from previous build")

    current_os_keys = expected_os_keys(repo_root, groups)

    print("=== Building SQLite database ===")
    for group in groups:
        group_dir = repo_root / group
        if not group_dir.exists():
            continue
        print(f"Importing group: {group}")
        import_data_repo(repo_root, str(db_path), group)

    reader = Reader(str(db_path))
    removed = reader.purge_missing_os(current_os_keys)
    for os_info in removed:
        print(
            f"Removed stale OS: {os_info['name']} {os_info['version']} {os_info['build']}"
        )

    print("\n=== Exporting JSON records ===")

    # Re-apply the pre-release flag from the raw list.json/meta.json so the
    # published data (and the frontend's Beta badge) keep it, even for
    # databases created before the os table had a beta column.
    beta_keys = set()
    for group in groups:
        for raw in load_group_os_list(repo_root, group):
            if raw.get("beta"):
                beta_keys.add((group, raw["version"], raw["build"]))

    group_builds: dict[str, list[dict]] = {g: [] for g in groups}
    for os_info in reader.all_os():
        tag = f"{os_info['version']}_{os_info['build']}"
        for g in groups:
            if (repo_root / g / tag).exists():
                if (g, os_info["version"], os_info["build"]) in beta_keys:
                    os_info["beta"] = True
                group_builds[g].append(os_info)
                break

    stats = export_records(str(db_path), group_builds, output)
    for group, counts in stats.items():
        print(f"  {group}: " + ", ".join(f"{v} {k}" for k, v in counts.items()))
    print(f"Output: {output}")


if __name__ == "__main__":
    main()
