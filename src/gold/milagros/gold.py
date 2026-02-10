from __future__ import annotations

import argparse
from pathlib import Path

from pyspark.sql import SparkSession, functions as F


def repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Milagros Gold Modeling (from Silver)")
    p.add_argument("--ingest-date", required=True, help="Partition date YYYY-MM-DD")
    return p.parse_args()


def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def write_parquet(df, out_dir: Path) -> None:
    ensure_dir(out_dir)
    (
        df.coalesce(1)
        .write.mode("overwrite")
        .parquet(str(out_dir))
    )


def main() -> None:
    args = parse_args()
    root = repo_root()

    silver_dir = root / "processed" / "silver" / "milagros" / f"ingest_date={args.ingest_date}"
    gold_dir = root / "processed" / "gold" / "milagros" / f"ingest_date={args.ingest_date}"

    print(f"[gold] repo_root={root}")
    print(f"[gold] reading_silver={silver_dir}")
    print(f"[gold] writing_gold={gold_dir}")

    spark = SparkSession.builder.appName("milagros-gold").getOrCreate()

    df = spark.read.parquet(str(silver_dir))

    # ---- Gold Table 1: births by municipio (top-level distribution)
    births_by_municipio = (
        df.groupBy("departamento_residencia", "municipio_residencia")
          .agg(F.count("*").alias("births"))
          .orderBy(F.desc("births"))
    )
    write_parquet(births_by_municipio, gold_dir / "births_by_municipio")

    # ---- Gold Table 2: births by sexo
    births_by_sexo = (
        df.groupBy("sexo")
          .agg(F.count("*").alias("births"))
          .orderBy(F.desc("births"))
    )
    write_parquet(births_by_sexo, gold_dir / "births_by_sexo")

    # ---- Gold Table 3: birth weight summary stats (overall)
    weight_summary = (
        df.select(F.col("peso_gramos").cast("double").alias("peso_gramos"))
          .where(F.col("peso_gramos").isNotNull())
          .agg(
              F.count("*").alias("n"),
              F.avg("peso_gramos").alias("avg_peso_gramos"),
              F.expr("percentile_approx(peso_gramos, 0.5)").alias("median_peso_gramos"),
              F.min("peso_gramos").alias("min_peso_gramos"),
              F.max("peso_gramos").alias("max_peso_gramos"),
          )
    )
    write_parquet(weight_summary, gold_dir / "weight_summary")

    # ---- Success marker
    success = gold_dir / "_SUCCESS"
    success.write_text("ok\n", encoding="utf-8")
    print(f"[gold] wrote success marker: {success}")

    spark.stop()
    print("[gold] done")


if __name__ == "__main__":
    main()