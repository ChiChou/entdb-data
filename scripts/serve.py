#!/usr/bin/env python3
"""Serve a build output directory locally, like the static host does.

    python scripts/serve.py build-output            # http://localhost:8787
    python scripts/serve.py build-output --port 9000

- gzip-compresses responses when the client accepts it (GitHub Pages does),
  so transfer sizes match production;
- sends CORS headers, so a dev frontend on another port can fetch directly;
- disables caching, so a rebuild shows up on the next request.
"""

import argparse
import gzip
import http.server
from functools import partial

COMPRESSIBLE = (".json", ".txt", ".xml", ".html", ".js", ".css")


class Handler(http.server.SimpleHTTPRequestHandler):
    extensions_map = {
        **http.server.SimpleHTTPRequestHandler.extensions_map,
        ".json": "application/json; charset=utf-8",
    }

    def end_headers(self):
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Cache-Control", "no-cache")
        super().end_headers()

    def do_GET(self):
        path = self.translate_path(self.path)
        accepts_gzip = "gzip" in self.headers.get("Accept-Encoding", "")
        if not (accepts_gzip and path.endswith(COMPRESSIBLE)):
            return super().do_GET()
        try:
            with open(path, "rb") as f:
                body = gzip.compress(f.read(), compresslevel=6)
        except (FileNotFoundError, IsADirectoryError):
            return super().do_GET()
        self.send_response(200)
        self.send_header("Content-Type", self.guess_type(path))
        self.send_header("Content-Encoding", "gzip")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Vary", "Accept-Encoding")
        self.end_headers()
        self.wfile.write(body)


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("directory", help="Build output directory")
    parser.add_argument("--port", type=int, default=8787)
    args = parser.parse_args()

    handler = partial(Handler, directory=args.directory)
    server = http.server.ThreadingHTTPServer(("127.0.0.1", args.port), handler)
    print(f"Serving {args.directory} on http://localhost:{args.port}")
    server.serve_forever()


if __name__ == "__main__":
    main()
