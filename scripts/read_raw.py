from __future__ import annotations

import argparse
import os
from urllib.parse import urlparse

import duckdb


def configure_s3(con: duckdb.DuckDBPyConnection) -> None:
    endpoint = os.getenv("S3_ENDPOINT", "localhost:8333")
    parsed = urlparse(endpoint if "://" in endpoint else f"http://{endpoint}")
    host_port = parsed.netloc
    access_key = os.getenv("S3_ACCESS_KEY", "s3admin")
    secret_key = os.getenv("S3_SECRET_KEY", "s3admin123")

    con.execute("INSTALL httpfs")
    con.execute("LOAD httpfs")
    con.execute("SET s3_region='us-east-1'")
    con.execute(f"SET s3_endpoint='{host_port}'")
    con.execute(f"SET s3_access_key_id='{access_key}'")
    con.execute(f"SET s3_secret_access_key='{secret_key}'")
    con.execute("SET s3_url_style='path'")
    con.execute("SET s3_use_ssl=false")


def main() -> None:
    parser = argparse.ArgumentParser(description="Read a raw CSV object from SeaweedFS with DuckDB")
    parser.add_argument("--path", required=True, help="s3://raw/... path to hour.csv")
    args = parser.parse_args()

    if not args.path.startswith("s3://"):
        raise SystemExit("--path должен начинаться с s3://")

    con = duckdb.connect()
    try:
        configure_s3(con)
        rows = con.execute("SELECT * FROM read_csv_auto(?) LIMIT 5", [args.path]).fetchall()
        columns = [item[0] for item in con.description]
        print("Columns:")
        print(columns)
        print("\nFirst 5 rows:")
        for row in rows:
            print(row)
    finally:
        con.close()


if __name__ == "__main__":
    main()
