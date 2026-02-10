import argparse
import os
from pathlib import Path

import pandas as pd
import pyarrow.dataset as ds
import psycopg


TABLES = [
    "births_by_municipio",
    "births_by_sexo",
    "weight_summary",
    "mother_age_bands",
    "father_age_bands",
]


def repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def read_parquet_dir(parquet_dir: Path) -> pd.DataFrame:
    dataset = ds.dataset(str(parquet_dir), format="parquet")
    table = dataset.to_table()
    return table.to_pandas()


def export_csv(df: pd.DataFrame, out_csv: Path) -> None:
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_csv, index=False)


def copy_csv(conn: psycopg.Connection, full_table: str, csv_path: Path) -> None:
    with conn.cursor() as cur:
        cur.execute(f"TRUNCATE TABLE {full_table};")
        with cur.copy(f"COPY {full_table} FROM STDIN WITH (FORMAT csv, HEADER true)") as copy:
            with csv_path.open("rb") as f:
                copy.write(f.read())


def main() -> None:
    parser = argparse.ArgumentParser(description="Load Gold parquet tables into PostgreSQL")
    parser.add_argument("--ingest-date", required=True, help="YYYY-MM-DD")
    args = parser.parse_args()

    root = repo_root()
    gold_base = root / "processed/gold/milagros" / f"ingest_date={args.ingest_date}"
    schema_sql = root / "src/warehouse/postgres/schema.sql"

    host = os.getenv("PGHOST", "localhost")
    port = int(os.getenv("PGPORT", "5433"))
    db = os.getenv("PGDATABASE", "milagros")
    user = os.getenv("PGUSER", "milagros")
    password = os.getenv("PGPASSWORD", "milagros")

    print(f"[pg] gold_base={gold_base}")
    print(f"[pg] host={host} port={port} db={db} user={user}")

    if not gold_base.exists():
        raise FileNotFoundError(f"Gold path not found: {gold_base}")
    if not schema_sql.exists():
        raise FileNotFoundError(f"Schema file not found: {schema_sql}")

    conn = psycopg.connect(host=host, port=port, dbname=db, user=user, password=password)
    conn.autocommit = True

    with conn.cursor() as cur:
        cur.execute(schema_sql.read_text(encoding="utf-8"))
    print("[pg] applied schema")

    export_base = root / "artifacts/exports/postgres" / f"ingest_date={args.ingest_date}"

    for t in TABLES:
        parquet_dir = gold_base / t
        if not parquet_dir.exists():
            raise FileNotFoundError(f"Missing gold table directory: {parquet_dir}")

        df = read_parquet_dir(parquet_dir)
        out_csv = export_base / f"{t}.csv"
        export_csv(df, out_csv)

        full_table = f"milagros.{t}"
        copy_csv(conn, full_table, out_csv)
        print(f"[pg] loaded {full_table} rows={len(df)}")

    conn.close()
    print("[pg] done")


if __name__ == "__main__":
    main()