"""Stage: Export SQLite database to static KV files."""

import json
from pathlib import Path

from indexer.db import Reader
from indexer.kv import KVStore


def export_static(db_path: str, output_dir: Path) -> list[str]:
    output_dir.mkdir(parents=True, exist_ok=True)
    reader = Reader(db_path)
    oslist = []
    written = []

    for os_info in reader.all_os():
        oslist.append(os_info)
        osid = os_info["id"]

        subdir = output_dir / f"{os_info['version']}_{os_info['build']}"
        subdir.mkdir(parents=True, exist_ok=True)
        written.append(subdir.name)

        with (subdir / "paths.txt").open("w") as fp:
            fp.write("\n".join(reader.paths_by_osid(osid)))

        with KVStore(subdir / "blobs.index.json", subdir / "blobs.txt") as blobs_store:
            for b in reader.binaries_by_osid(osid):
                blobs_store.add(b["path"], b["xml"])

        with KVStore(subdir / "keys.index.json", subdir / "keys.txt") as keys_store:
            for key in reader.keys_by_osid(osid):
                paths = reader.owns_key_by_osid(osid, key)
                keys_store.add(key, "\n".join(paths).encode())

        with open(subdir / "meta.json", "w") as fp:
            json.dump(os_info, fp)

    with (output_dir / "list.json").open("w") as fp:
        json.dump(oslist, fp)

    return written


def main():
    import argparse

    parser = argparse.ArgumentParser(
        description="Export database to static KV files"
    )
    parser.add_argument("db", help="Path to SQLite database")
    parser.add_argument("output", help="Output directory")
    args = parser.parse_args()

    written = export_static(args.db, Path(args.output))
    print(f"Exported {len(written)} versions: {', '.join(written)}")


if __name__ == "__main__":
    main()
