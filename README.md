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
into a SQLite database (`ent.db`, published as a release asset) and the
static JSON records the web frontend reads (deployed to GitHub Pages):

| File | Contents |
|------|----------|
| `groups.json` | Platform groups |
| `{group}/list.json` | Builds, sorted by version (a build's index is its *ordinal*) |
| `{group}/b/{version}_{build}.json` | One build's binary paths and keys |
| `{group}/k/{hash(key)}.json` | One key across all builds: which binaries have it, with which value |
| `{group}/p/{hash(path)}.json` | One binary across all builds: its entitlements (original XML) |

Records store inclusive ordinal ranges instead of repeating data per build;
`hash` is 64-bit FNV-1a of the UTF-8 name (16 hex digits). See
`scripts/stages/export_records.py`.

Build and serve locally:

```sh
python scripts/build.py -o build-output   # add --previous DIR to reuse an ent.db
python scripts/serve.py build-output      # http://localhost:8787, gzip + CORS
```

Updated automatically by [entdb-indexer](https://github.com/ChiChou/entdb-indexer).

## Related Repositories

| Repository | Description |
|------------|-------------|
| [entdb](https://github.com/ChiChou/entdb) | Web frontend |
| [entdb-indexer](https://github.com/ChiChou/entdb-indexer) | Indexer workflow |

## License

MIT
