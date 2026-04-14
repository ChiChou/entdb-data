import plistlib
import json
import sqlite3

from pathlib import Path


class Writer:
    def __init__(
        self,
        path: str,
        name: str,
        build: str,
        version: str,
        devices: list[str] | None = None,
    ):
        self.path = path
        self.devices = devices or []
        self.conn = sqlite3.connect(self.path)
        self._existing_paths: set[str] | None = None

        self.create_tables()
        self.osid, self.os_exists = self._insert_os(name, version, build)

    def create_tables(self):
        sql_file = Path(__file__).parent / "schema.sql"

        with open(sql_file, "r") as fp:
            sql_script = fp.read()

        self.conn.executescript(sql_script)
        self.conn.commit()

    def _insert_os(self, name: str, version: str, build: str) -> tuple[int, bool]:
        cursor = self.conn.execute(
            "SELECT id FROM os WHERE name=? AND version=? AND build=?",
            (name, version, build),
        )
        row = cursor.fetchone()
        if row:
            osid, *_ = row
            return osid, True

        cursor = self.conn.execute(
            "INSERT INTO os (name, version, build, devices) VALUES (?, ?, ?, ?)",
            (name, version, build, json.dumps(self.devices)),
        )
        self.conn.commit()
        osid = cursor.lastrowid
        assert osid is not None, "Failed to insert OS entry"
        return osid, False

    def existing_paths(self) -> set[str]:
        if self._existing_paths is None:
            cursor = self.conn.execute(
                "SELECT path FROM bin WHERE osid=?",
                (self.osid,),
            )
            self._existing_paths = {row[0] for row in cursor.fetchall()}
        return self._existing_paths

    def insert(self, path: str, xml: bytes) -> bool:
        if not len(xml):
            return False

        if path in self.existing_paths():
            return False

        d = plistlib.loads(xml)

        cursor = self.conn.execute(
            "INSERT INTO bin (osid, path, xml) VALUES (?, ?, ?)",
            (self.osid, path, xml),
        )

        binid = cursor.lastrowid
        assert binid is not None, "failed to insert row for path {path}"

        for key, val in d.items():
            self.conn.execute(
                "INSERT INTO pair (binid, key, value) VALUES (?, ?, ?)",
                (binid, key, json.dumps(val)),
            )

        self.conn.commit()
        self.existing_paths().add(path)
        return True


class Reader:
    def __init__(self, path: str):
        self.path = path
        self.conn = sqlite3.connect(self.path)

    def all_os(self):
        cursor = self.conn.execute("SELECT id, name, version, build, devices FROM os")
        return [
            dict(
                id=osid,
                name=name,
                version=version,
                build=build,
                devices=json.loads(devices),
            )
            for osid, name, version, build, devices in cursor.fetchall()
        ]

    def purge_missing_os(self, expected_keys: set[tuple[str, str, str]]) -> list[dict]:
        removed = []

        for os_info in self.all_os():
            os_key = (os_info["name"], os_info["version"], os_info["build"])
            if os_key in expected_keys:
                continue

            self.conn.execute(
                "DELETE FROM pair WHERE binid IN (SELECT id FROM bin WHERE osid=?)",
                (os_info["id"],),
            )
            self.conn.execute(
                "DELETE FROM bin WHERE osid=?",
                (os_info["id"],),
            )
            self.conn.execute(
                "DELETE FROM os WHERE id=?",
                (os_info["id"],),
            )
            removed.append(os_info)

        if removed:
            self.conn.commit()

        return removed

    def paths_by_osid(self, osid: int) -> list[str]:
        cursor = self.conn.execute(
            "SELECT path FROM bin WHERE osid=?",
            (osid,),
        )
        return [row[0] for row in cursor.fetchall()]

    def binaries_by_osid(self, osid: int):
        cursor = self.conn.execute(
            "SELECT path, xml FROM bin WHERE osid=?",
            (osid,),
        )
        return [dict(path=path, xml=xml) for path, xml in cursor.fetchall()]

    def keys_by_osid(self, osid: int):
        cursor = self.conn.execute(
            """
            SELECT distinct key FROM pair
            JOIN bin ON pair.binid=bin.id
            WHERE bin.osid=?""",
            (osid,),
        )
        return [row[0] for row in cursor.fetchall()]

    def owns_key_by_osid(self, osid: int, key: str) -> list[str]:
        cursor = self.conn.execute(
            """
            SELECT path FROM bin JOIN pair ON bin.id=pair.binid
            WHERE bin.osid=? AND pair.key=?""",
            (osid, key),
        )
        return [row[0] for row in cursor.fetchall()]

    def metadata(self, build: str):
        cursor = self.conn.execute(
            "SELECT name, version, devices FROM os WHERE build=?", (build,)
        )
        name, version, devices = cursor.fetchone()
        return dict(
            name=name, build=build, version=version, devices=json.loads(devices)
        )

    def paths(self, build: str) -> list[str]:
        cursor = self.conn.execute(
            """
            SELECT path FROM bin JOIN os ON bin.osid=os.id
            WHERE os.build=?""",
            (build,),
        )
        return [row[0] for row in cursor.fetchall()]

    def binaries(self, osbuild: str):
        cursor = self.conn.execute(
            """
            SELECT path, xml FROM bin JOIN os ON bin.osid=os.id
            WHERE os.build=?""",
            (osbuild,),
        )
        return [
            dict(path=path, xml=xml) for path, xml in cursor.fetchall()
        ]

    def owns_key(self, osbuild: str, key: str) -> list[str]:
        cursor = self.conn.execute(
            """
            SELECT path FROM bin JOIN pair ON bin.id=pair.binid
            JOIN os ON bin.osid=os.id WHERE os.build=? AND pair.key=?""",
            (osbuild, key),
        )
        return [row[0] for row in cursor.fetchall()]

    def keys(self, build: str):
        cursor = self.conn.execute(
            """
            SELECT distinct key FROM pair JOIN bin ON pair.binid=bin.id
            JOIN os ON bin.osid=os.id WHERE os.build=?""",
            (build,),
        )
        return [row[0] for row in cursor.fetchall()]

    def known_builds(self) -> set[str]:
        cursor = self.conn.execute("SELECT build FROM os")
        return {row[0] for row in cursor.fetchall()}
