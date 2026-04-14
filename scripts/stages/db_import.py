"""Stage: Import XML entitlement files into SQLite database."""

from pathlib import Path

from indexer.db import Writer


def import_xml_dir(
    xml_dir: Path,
    db_path: str,
    name: str,
    version: str,
    build: str,
    devices: list[str] | None = None,
) -> int:
    writer = Writer(db_path, name, build, version, devices)
    if writer.os_exists:
        return 0

    bin_dir = xml_dir / "bin"
    count = 0

    paths_file = xml_dir / "paths.txt"
    if not paths_file.exists():
        return 0

    for line in paths_file.read_text().splitlines():
        line = line.strip()
        if not line:
            continue

        xml_path = bin_dir / line.lstrip("/")
        xml_path = xml_path.with_suffix(xml_path.suffix + ".xml")

        if not xml_path.exists():
            continue

        xml = xml_path.read_bytes()
        if writer.insert(line, xml):
            count += 1

    return count


def import_data_repo(data_repo: Path, db_path: str, group: str) -> dict[str, int]:
    import json

    group_dir = data_repo / group
    list_path = group_dir / "list.json"
    counts: dict[str, int] = {}

    if list_path.exists():
        with list_path.open() as f:
            os_list = json.load(f)
    else:
        os_list = []
        for d in sorted(group_dir.iterdir()):
            meta = d / "meta.json"
            if meta.exists():
                with meta.open() as f:
                    os_list.append(json.load(f))

    for os_info in os_list:
        version_dir = group_dir / f"{os_info['version']}_{os_info['build']}"
        if not version_dir.exists():
            continue

        count = import_xml_dir(
            version_dir,
            db_path,
            os_info["name"],
            os_info["version"],
            os_info["build"],
            os_info.get("devices"),
        )
        counts[f"{group}/{version_dir.name}"] = count
        print(f"  {version_dir.name}: {count} binaries")

    return counts


def main():
    import argparse
    import json

    parser = argparse.ArgumentParser(description="Import XML files into SQLite database")
    parser.add_argument("xml_dir", help="Directory containing XML entitlement files")
    parser.add_argument("-o", "--output", required=True, help="SQLite database path")
    parser.add_argument("--name", required=True, help="OS name (e.g., 'iOS 18.4')")
    parser.add_argument("--version", required=True, help="OS version (e.g., '18.4')")
    parser.add_argument("--build", required=True, help="Build ID (e.g., '22E240')")
    parser.add_argument("--devices", help="Comma-separated device identifiers")
    args = parser.parse_args()

    devices = args.devices.split(",") if args.devices else None
    count = import_xml_dir(
        Path(args.xml_dir),
        args.output,
        args.name,
        args.version,
        args.build,
        devices,
    )
    print(json.dumps({"imported": count}))


if __name__ == "__main__":
    main()
