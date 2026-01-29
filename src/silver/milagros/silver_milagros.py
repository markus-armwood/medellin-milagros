# src/silver/milagros/silver_milagros.py

from __future__ import annotations
# without .
# As projects grow, it’s common to:
#    •   reference classes or types defined in other files
#    •   create circular imports accidentally
# __future__ annotations prevents an error at import time because of these issues.
import os
import re
# regular expressions
import unicodedata
# dealing with special characters
from datetime import date
from pathlib import Path

import pandas as pd


def clean_col(name: str) -> str:
    # with docstring """ the comment in between can be accessed with help() at runtime
    """
    Convert column names to a stable, SQL-friendly format:
    - strip whitespace
    - lowercase
    - remove accents
    - spaces -> underscores
    - remove non-alphanumeric (except underscore)
    - collapse repeated underscores
    """
    name = name.strip().lower()
    # removes white space before and after the string and makes all letters lowercase.
    name = unicodedata.normalize("NFKD", name)
    # Computers store accented characters in multiple possible Unicode forms...
    # ...Normalization via ".normalize" converts them into a standard, predictable representation.
    # unicodedata.normalize("NFKD", name) turns accented characters in the "name" column...
    # ...into “letter + accent” so the accent can be safely removed.
    # There are 4 different options NFKD, NFKC, 
        # NFDK (Normalized Form Compatibility Decomposed) splits accents into base letter...
        # ... + accent and simplifies symbols into plain text equivalent
            # NFDK for machine-safe identifiers
            # I'm using it for column names, file paths, schemas, SQL identifiers
        # NFKC (Normalized Form Compatibility Composed) keeps accents composed but... 
            # NFKC for clean user input
        # ...replaces symbols with plain text equivalent 
        # NFD (Normalize Form Decomposed) splits accents into base letter + accent and does...
        #... not simplify symbols
            # Use NFD for Linguistic processing
        # NFC (Normalize Form Composed) keeps accented characters as single character
            # Us NFC for human-readable text
    name = "".join(ch for ch in name if not unicodedata.combining(ch))
        # This code loops through every charcter in "name" and checks if it is...
        # ... not a normal letter, then joins the letter if it is a normal letter only...
        # ... otherwise the non-normal letter/character gets dropped (accents)
        # The first ch before the "for" is the expression being returned f
        # The first ch is what gets appended after the iteration.
        # The ch answers “What should I output for each element that passes the filter?”
        # The general form for this is 
        # ("expression" for "variable" in "iterable" if "condition")
        # unicodedata.combining(ch) is greater than 0 if it is an accent
        # unicodedata.combining(ch) is 0 if it is a normal letter
    name = re.sub(r"\s+", "_", name)
    # "r" means use raw strings for regex patterns, otherwise \ will be interpreted...
    # ... as an escape character or line continuation.
    # An escape character is a special character (commonly \) used inside a string...
    # ...to indicate that the following character should be interpreted in a special way, not literally.
        # examples are "\n" for new line, "\t" for tab, "\" for double quote, and...
        # ... "\\" for literal backslash
        # Line continuation allows a single logical statement...
        # ... to span multiple lines of code.
    # regular expression, substitute, (pattern, replacement, string being searched and modified)
    # (\s) means replace any whitespace, (+) means one or more. 
    name = re.sub(r"[^a-z0-9_]", "", name)
    # ^ means NOT
    # This logic removes every character from name that is not a lowercase letter, number, or underscore.
    name = re.sub(r"_+", "_", name)
    # replace characters that are one or more underscores with one underscore.
    return name.strip("_")
    # takes aware all underscores before and after the newly modified name and returns the it


def main() -> None:
    # ----- Config -----
    raw_base = Path(os.getenv("RAW_BASE_PATH", "raw/milagros")).resolve()
    # used in the pathlib library to transform a relative file path into an absolute, physical path
    silver_base = Path(os.getenv("SILVER_BASE_PATH", "processed/silver/milagros")).resolve()

    ingest_date = os.getenv("INGEST_DATE", "").strip() or date.today().isoformat()
    # Use the INGEST_DATE environment variable if it exists and isn’t blank; otherwise, use today’s date
    raw_file = raw_base / f"ingest_date={ingest_date}" / "Nacimientos_HGM.xlsx"
    # ets you embed Python expressions directly inside a string; in this case {ingest_date}...
    # ...the received value of {ingest_date} is put in the string
    silver_out_dir = silver_base / f"ingest_date={ingest_date}"
    silver_out_dir.mkdir(parents=True, exist_ok=True)
    # Create the output directory for this ingest date, creating...
    # ...any missing parent directories, and don’t fail if it already exists (exist_ok=True)

    out_parquet = silver_out_dir / "milagros_hgm.parquet"
    # the cleaned column name table is the parquet file
    success_marker = silver_out_dir / "_SUCCESS"

    # ----- Logging -----
    print(f"[silver] RAW_FILE={raw_file}")
    print(f"[silver] OUT_PARQUET={out_parquet}")

    # ----- Validate raw exists -----
    if not raw_file.exists():
        raise FileNotFoundError(f"Raw XLSX not found: {raw_file}")

    # ----- Read Excel (first sheet by default) -----
    df = pd.read_excel(raw_file, sheet_name="Nacimientos")

    # ----- Standardize column names -----
    df.columns = [clean_col(c) for c in df.columns]

    # ----- Standardize values -----
    # 1) Trim string-like columns
    for c in df.columns:
        if df[c].dtype == "object":
            df[c] = df[c].astype("string").str.strip()

    # 2) Empty strings -> NA
    df = df.replace({"": pd.NA})

    # 3) If there are any "Unnamed: x" columns (common in Excel), drop them
    unnamed_cols = [c for c in df.columns if c.startswith("unnamed")]
    if unnamed_cols:
        df = df.drop(columns=unnamed_cols)



    # ----- Silver type hardening -----

    # Integers
    int_cols = ["ano", "periodo_de_reporte"]
    for c in int_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")

    # Nullable integer ages
    age_cols = ["edad_madre", "edad_padre"]
    for c in age_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")

    # Key categoricals
    cat_cols = [
        "sexo",
        "nivel_educativo_madre",
        "nivel_educativo_padre",
        "profesion_certificador",
    ]
    for c in cat_cols:
        if c in df.columns:
            df[c] = df[c].astype("string")




 # ----- Silver contract checks (Step 4.10) -----

    # Step 10.1: Required columns
    required_cols = [
        "ano",
        "periodo_de_reporte",
        "sexo",
        "fecha_nacimiento",
        "edad_madre",
        "municipio_residencia",
        "edad_padre",
    ]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns in Silver: {missing}")

    # Step 10.2: Sanity checks
    if "edad_madre" in df.columns:
        s = df["edad_madre"].dropna()
        if ((s < 10) | (s > 60)).any():
            raise ValueError("edad_madre outside expected range (10–60)")

    if "ano" in df.columns:
        s = df["ano"].dropna()
        if ((s < 2000) | (s > 2035)).any():
            raise ValueError("ano outside expected range (2000–2035)")





    # ----- Write Silver Parquet -----
    df.to_parquet(out_parquet, index=False)
    success_marker.touch()

    # ----- Minimal verification logs -----
    print(f"[silver] rows={len(df)} cols={len(df.columns)}")
    print(f"[silver] wrote={out_parquet}")
    print(f"[silver] wrote={success_marker}")
    print("[silver] columns:", df.columns.tolist())


if __name__ == "__main__":
    main()