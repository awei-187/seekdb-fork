"""Generate independent Parquet reader fixtures with PyArrow (test dependency only).

Usage: python generate_parquet_fixtures.py OUTPUT_DIRECTORY
Run file_scan_tests OUTPUT_DIRECTORY to validate the generated reference files.
"""
import argparse
import base64
import datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    table = pa.table({
        "id": pa.array([1, 2, 3, 4, 5, 6, 7], type=pa.int64()),
        "flag": [True, False, None, True, False, True, None],
        "score": pa.array([1.5, None, 3., 4.25, 5.5, 6., 7.], type=pa.float64()),
        "text": ["a", "b", None, "a", "你好", "x\ny", ""],
        "day": pa.array([
            datetime.date(2024, 2, 29), None, datetime.date(1970, 1, 1),
            datetime.date(1969, 12, 31), datetime.date(2025, 1, 2), None,
            datetime.date(2026, 11, 1),
        ], type=pa.date32()),
        "stamp": pa.array([
            datetime.datetime(2024, 1, 1, 1, 2, 3, 456789), None,
            datetime.datetime(1970, 1, 1),
            datetime.datetime(1969, 12, 31, 23, 59, 59, 999999),
            datetime.datetime(2025, 1, 2), None, datetime.datetime(2026, 11, 1),
        ], type=pa.timestamp("us")),
    })
    for version in ["1.0", "2.0"]:
        for codec in ["NONE", "snappy"]:
            for dictionary in [False, True]:
                pq.write_table(
                    table, args.output / f"{version}-{codec}-{dictionary}.parquet",
                    data_page_version=version, compression=codec,
                    use_dictionary=dictionary, row_group_size=3,
                )
    root = Path(__file__).resolve().parents[4]
    official = root / "tools/deploy/mysql_test/test_suite/file_sql/data/simple.parquet.b64"
    (args.output / "official.parquet").write_bytes(base64.b64decode(official.read_text()))
    limits = args.output / "limits"
    limits.mkdir(exist_ok=True)
    pq.write_table(pa.table({"id": list(range(32)), "large": ["x" * (1024 * 1024)] * 32}),
                   limits / "dictionary-expansion.parquet", compression="snappy", use_dictionary=True)
    print("Generated 9 Parquet reference files")


if __name__ == "__main__":
    main()
