import argparse
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq


def convert_table_to_us(table: pa.Table) -> pa.Table:
    cols = []
    names = table.schema.names

    for name in names:
        arr = table[name]
        t = arr.type

        # Convert timestamp(ns) -> timestamp(us), preserve timezone if present
        if pa.types.is_timestamp(t) and t.unit == "ns":
            arr = arr.cast(pa.timestamp("us", tz=t.tz))

        cols.append(arr)

    return pa.table(cols, names=names)


def convert_folder(in_dir: Path, out_dir: Path) -> None:
    in_dir = in_dir.resolve()
    out_dir = out_dir.resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    files = sorted(in_dir.rglob("*.parquet"))
    if not files:
        raise FileNotFoundError(f"No .parquet files found under: {in_dir}")

    for i, f in enumerate(files, 1):
        table = pq.read_table(f)
        table_us = convert_table_to_us(table)

        out_file = out_dir / f.name
        pq.write_table(table_us, out_file)
        print(f"[{i}/{len(files)}] wrote {out_file}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="inp", required=True, help="Input parquet folder")
    ap.add_argument("--out", dest="out", required=True, help="Output parquet folder")
    args = ap.parse_args()

    convert_folder(Path(args.inp), Path(args.out))


if __name__ == "__main__":
    main()