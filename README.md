# entdb-data

Raw entitlement data repository for Apple platforms.

**Browse online:** [https://chichou.github.io/entdb](https://chichou.github.io/entdb)

## Structure

Contains XML entitlement files organized by platform group:

- `iOS/` — iPhone/iPad entitlements
- `mac/` — Apple Silicon Mac entitlements (macOS 11+)
- `osx/` — Intel Mac entitlements (OS X 10.7 – 10.15)

Each version directory contains:
- `meta.json` — OS metadata
- `paths.txt` — list of binary paths
- `bin/` — XML entitlement files (mirrors Apple's filesystem hierarchy)

The `scripts/` directory contains the build pipeline that converts raw XML
into a SQLite database and static KV files for the web frontend.

Updated automatically by [entdb-indexer](https://github.com/ChiChou/entdb-indexer).

## Related Repositories

| Repository | Description |
|------------|-------------|
| [entdb](https://github.com/ChiChou/entdb) | Web frontend |
| [entdb-indexer](https://github.com/ChiChou/entdb-indexer) | Indexer workflow |

## License

MIT
