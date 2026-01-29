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
        # raise stops program and returns the error

    # ----- Read Excel  -----
    df = pd.read_excel(raw_file, sheet_name="Nacimientos")
    # (location, which sheet to read)

    # ----- Standardize column names -----
    df.columns = [clean_col(c) for c in df.columns]
    # dr.columns is the list-like object that holds all column names of the DataFrame.
    # c is the index that lets us know which entry we're on for df.column
    # puts the column name at the entry of index c into clean_col function defined above
    # the list of clean_col function outputs is an anonymous temporary list object...
    # ...stored in memory.  This is filled up with the results of the list as it is being..
    #... iterated through in clean_col, then sets dr.columns equal to the values of the anonymous...
    #... temporary list when the loop has completed.

    # ----- Standardize values -----
    # 1) Trim string-like columns
    for c in df.columns:
    # loop through each column name at the index c
        if df[c].dtype == "object":
        # at column (aka Series) index c of this DataFrame (aka Table)
        # df[c] just takes the column/Series of the DataFrame/Table specifically at index c 
        # This part of the code checks if the Data types of the values to see if it an object
        # object is pandas signal that the values in the column are text-like..
        # ...and we can only perform string operations on text.
        # We check if it's an object because .strip() only works on strings mixed values (strings + blanks)...
        # ...sometimes numbers stored as text
        # ABOUT OBJECTS in DataFrames:
            # Objects behave like strings but are not guaranteed to be strings
            # Objects could be:
                # strings ("FEMENINO")
                # mixed strings + blanks
                # numbers stored as text ("34")
                # mixed types ("34", None, " ")
            # the .str below ensures that all values in the DataFrame are stored as strings and not a mixture  
            # with an object a value of NaN is a float and None is and Object, this is inconsistent 
            # with dtype string, null/missing values are stored as <NA> which is consistent.

            df[c] = df[c].astype("string").str.strip()
            # .astype("string") converts values to pandas nullable string dtype...
            #...Preserves <NA> cleanly and ensures .str methods behave consistently
            # string values in DataFrame that get converted to <NA>:
                # None,NaN / numpy.nan, pd.NA (already missing)
            
            # string values in DataFrame that DO NOT get converted to <NA>:
                # These stay as valid strings unless you explicitly convert them:
                # "" (empty string)
                # " " (spaces)
                # "\t", "\n" (whitespace)
                # "NA", "null", "None" (literal text)    


    # 2) Empty strings -> NA
    df = df.replace({"": pd.NA})
        # The dictionary {"": pd.NA} means:
            # Find: "" (empty string)
            # Replace with: pd.NA (pandas missing value)
        # The dictionary is used as input into the .replace only to show that this value change was...
        #... a part of the Data Contract and not an ad hoc fix.



    # 3) If there are any "Unnamed: x" columns (common in Excel), drop them
        # When pandas reads Excel (or CSV) files, it auto-generates column names...
        #...for columns that don’t have a header. Those generated names look like
            # Examples: 
                # "Unnamed: 0"
                # "Unnamed: 1"
                # "Unnamed: 2"
            #After clean_col function, these become...
                # 'unnamed_0'
                # 'unnamed_1'
                # 'unnamed_2' 
            # Now we can search for columns that start with "unnamed" by using .startwith to delete them...
            #... since they hold now values that we want

    unnamed_cols = [c for c in df.columns if c.startswith("unnamed")]
    if unnamed_cols:
        df = df.drop(columns=unnamed_cols)
    # so this loops through the dataframe columns at index c and finds columns that begin with unnamed...
    #...then stores them in a list called unnamed_cols.  then checks if that unnamed_cols list exists...
    #...if it does then it drops all columns in the dataframe that match the values in the unnamed_cols list



#################### START HERE TOMORROW 30 JAN 2026##########

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