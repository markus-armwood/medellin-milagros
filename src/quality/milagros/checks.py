# /Users/markusallenarmwood/medellin-milagros/src/quality/milagros/checks.py

import os
import argparse
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim
from pyspark.errors.exceptions.captured import AnalysisException


# ----------------------------
# Spark setup
# ----------------------------
def build_spark(app_name: str) -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.warehouse.dir", "file:/tmp/spark-warehouse")
        .config("spark.hadoop.fs.defaultFS", "file:///")
        .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem")
        .getOrCreate()
    )


# ----------------------------
# Repo + path helpers
# ----------------------------
def repo_root() -> str:
    # src/quality/milagros/checks.py -> repo root is ../../..
    return os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))


def silver_partition_uri(root: str, ingest_date: str) -> str:
    return f"file:{root}/processed/silver/milagros/ingest_date={ingest_date}"


def _file_uri_to_path(uri: str) -> Path:
    if uri.startswith("file:"):
        uri = uri.replace("file://", "").replace("file:", "")
    return Path(uri)


def _preview_dir(p: Path, limit: int = 50) -> str:
    if not p.exists():
        return "  (missing)"
    items = []
    for child in sorted(p.rglob("*")):
        if child.is_file():
            items.append(f"  - {str(child.relative_to(p))}")
        if len(items) >= limit:
            break
    return "\n".join(items) if items else "  (empty)"


# ----------------------------
# Robust parquet reader
# ----------------------------
def read_silver_parquet(spark: SparkSession, file_uri: str):
    local_path = _file_uri_to_path(file_uri)

    if not local_path.exists():
        raise FileNotFoundError(f"[DQ ERROR] Silver path does not exist:\n  {local_path}")

    parquet_files = list(local_path.rglob("*.parquet"))
    if not parquet_files:
        raise FileNotFoundError(
            f"[DQ ERROR] No Parquet files found under:\n  {local_path}\n\n"
            f"Directory contents:\n{_preview_dir(local_path)}"
        )

    try:
        return spark.read.option("recursiveFileLookup", "true").parquet(file_uri)
    except AnalysisException as e:
        msg = str(e)
        if "PARQUET_TYPE_ILLEGAL" in msg and "TIMESTAMP(NANOS" in msg:
            raise RuntimeError(
                "[DQ ERROR] Spark cannot read this Parquet because it contains nanosecond timestamps:\n"
                "  TIMESTAMP(NANOS,false)\n\n"
                "Fix: rewrite timestamps to microseconds before reading.\n"
            ) from e
        raise


def fail_if(condition: bool, message: str):
    if condition:
        raise Exception(message)


def main():
    parser = argparse.ArgumentParser(description="Milagros Silver Data Quality Checks")
    parser.add_argument(
        "--ingest-date",
        default=os.environ.get("INGEST_DATE", "2026-01-28"),
        help="Partition date YYYY-MM-DD",
    )
    args = parser.parse_args()

    ingest_date = args.ingest_date

    spark = build_spark("milagros-dq")
    spark.sparkContext.setLogLevel(os.environ.get("SPARK_LOG_LEVEL", "ERROR"))

    root = repo_root()
    uri = silver_partition_uri(root, ingest_date)

    print(f"[DQ] Repo root: {root}")
    print(f"[DQ] Reading Silver parquet: {uri}")

    df = read_silver_parquet(spark, uri)

    # ----------------------------
    # Phase 5 checks
    # ----------------------------

    # 5.3 Volume: not empty
    row_count = df.count()
    fail_if(row_count <= 0, "[DQ FAIL] Silver dataset is empty (0 rows)")
    print(f"[DQ PASS] Volume: {row_count} rows")

    # 5.2 Schema contract (STRICT — based on your actual Silver schema)
    expected_types = {
        "ano": "bigint",
        "periodo_de_reporte": "bigint",
        "sexo": "string",
        "peso_gramos": "bigint",
        "talla_centimetros": "bigint",
        "fecha_nacimiento": "timestamp_ntz",
        "parto_atendido_por": "string",
        "tiempo_de_gestacion": "double",
        "numero_consultas_prenatales": "bigint",
        "tipo_parto": "string",
        "multiplicidad_embarazo": "string",
        "apgar1": "double",
        "apgar2": "double",
        "grupo_sanguineo": "string",
        "factor_rh": "string",
        "pertenencia_etnica": "string",
        "edad_madre": "bigint",
        "estado_conyugal_madre": "string",
        "nivel_educativo_madre": "string",
        "ultimo_ano_aprobado_madre": "double",
        "pais_residencia": "string",
        "departamento_residencia": "string",
        "municipio_residencia": "string",
        "area_residencia": "string",
        "localidad": "string",
        "numero_hijos_nacidos_vivos": "bigint",
        "numero_embarazos": "bigint",
        "regimen_seguridad": "string",
        "nombre_administradora": "string",
        "edad_padre": "bigint",
        "nivel_educativo_padre": "string",
        "profesion_certificador": "string",
    }

    actual_types = dict(df.dtypes)

    missing = [c for c in expected_types if c not in actual_types]
    fail_if(len(missing) > 0, f"[DQ FAIL] Missing required columns: {missing}")

    unexpected = sorted([c for c in actual_types if c not in expected_types])
    fail_if(len(unexpected) > 0, f"[DQ FAIL] Unexpected columns found (schema drift): {unexpected}")

    for c, exp in expected_types.items():
        act = actual_types[c]
        fail_if(act != exp, f"[DQ FAIL] Type mismatch for {c}: expected {exp}, got {act}")
        print(f"[DQ PASS] Schema: {c} ({act})")

    # ----------------------------
    # Completeness (with threshold allowance)
    # ----------------------------
    required_non_null = ["sexo", "fecha_nacimiento", "edad_madre", "municipio_residencia"]

    # Allow tiny null rates for real-world source imperfections
    max_null_rate = 0.001  # 0.1%
    max_null_abs = 0       # keep 0; rate is the real guard

    for c in required_non_null:
        nulls = df.filter(col(c).isNull()).count()
        null_rate = nulls / row_count

        fail_if(
            (nulls > max_null_abs) and (null_rate > max_null_rate),
            f"[DQ FAIL] Nulls in {c}: {nulls} ({null_rate:.4%}) exceeds allowed rate {max_null_rate:.2%}",
        )
        print(f"[DQ PASS] Completeness: {c} (nulls={nulls}, rate={null_rate:.4%})")

    # ----------------------------
    # Domain: sexo (trimmed) — Option 1 includes INDETERMINADO
    # ----------------------------
    allowed_sexo = {"FEMENINO", "MASCULINO", "INDETERMINADO"}

    bad_sexo_df = df.filter(
        col("sexo").isNull() | (~trim(col("sexo")).isin(*sorted(list(allowed_sexo))))
    )
    bad_sexo = bad_sexo_df.count()

    if bad_sexo > 0:
        print("\n[DQ DEBUG] Sample invalid sexo values:")
        bad_sexo_df.select(
            "sexo",
            "ano",
            "periodo_de_reporte",
            "fecha_nacimiento",
            "edad_madre",
            "municipio_residencia",
        ).show(25, truncate=False)

    fail_if(bad_sexo > 0, f"[DQ FAIL] Invalid sexo values: {bad_sexo}")
    print("[DQ PASS] Domain: sexo in {FEMENINO,MASCULINO,INDETERMINADO} (trimmed)")

    # ----------------------------
    # Range: edad_madre
    # ----------------------------
    bad_edad_madre = df.filter((col("edad_madre") < 10) | (col("edad_madre") > 60)).count()
    fail_if(bad_edad_madre > 0, f"[DQ FAIL] edad_madre outside [10,60]: {bad_edad_madre}")
    print("[DQ PASS] Range: edad_madre within [10,60]")

    print("\n✅ PHASE 5 — DATA QUALITY CHECKS PASSED")
    spark.stop()


if __name__ == "__main__":
    main()