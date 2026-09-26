"""Run with `python -m examples.basic_usage`; no network or persistent writes."""

from contextlib import closing
import gzip
import hashlib
from pathlib import Path
from tempfile import TemporaryDirectory

from datacache import Cache, fetch_and_transform, fetch_csv_db


def main():
    with TemporaryDirectory() as directory:
        root = Path(directory)
        data = b"id,label\n1,case\n2,control\n"
        source = root / "records.csv.gz"
        source.write_bytes(gzip.compress(data))
        cache = Cache(cache_root=root / "cache")
        digest = hashlib.sha256(data).hexdigest()
        path = cache.fetch(source.as_uri(), filename="records.csv", expected_sha256=digest)
        assert Path(path).read_bytes() == data
        assert cache.inspect(filename="records.csv", expected_sha256=digest).verified

        with closing(fetch_csv_db(
                "records", source.as_uri(), csv_filename="records.csv",
                download_options={"cache_root": root / "cache", "expected_sha256": digest})) as connection:
            assert connection.execute("SELECT * FROM records").fetchall() == [(1, "case"), (2, "control")]

        def transform(source_path, output_path):
            text = Path(source_path).read_text().upper()
            Path(output_path).write_text(text)
            return text

        transformed = fetch_and_transform(
            "uppercase.csv", transform, lambda p: Path(p).read_text(),
            "records.csv", source.as_uri(), cache_root=root / "cache")
        assert transformed == data.decode().upper()
        source.unlink()
        assert cache.fetch(source.as_uri(), filename="records.csv") == path
        print("Verified download, SQLite cache, transformation, and offline reuse succeeded.")


if __name__ == "__main__":
    main()
