# src/ingestion/births/births.py

import os # By using the "import os" module's standardized interface so a
          # developer can write a script once and have it work consistently 
          # regardless of the underlying operating system; which makes the code portable. 
import shutil
# shutil mean "shell utilities and is Python’s built-in tool for safely copying, moving, and managing files and directories in data pipelines.
# Data Engineers use it because...
    # It works the same on all OSes
    # Is safer than shelling out to cp or mv
    # Is easier to test
    # Plays well with Python pipelines
from pathlib import Path  #pathlib lets you treat files and folders as objects, not strings.
                            #lets you build, inspect, and manage filesystem paths safely, 
                            #cleanly, and portably, which is why it’s everywhere in modern Python and data engineering pipelines.
from datetime import date #datetime is a class representing a calendar date only YYYY-MM-DD
                            #this is used for Folder partitioning: ingest_date=2026-01-26  

def main() -> None:  #This function intentionally does't return anything, it automatically returns None
    # --- Environment config ---
    raw_base_path = Path(os.getenv("RAW_BASE_PATH", "raw/milagros")).resolve()
    # os.getenv reads an environment variable
        # "RAW_BASE_PATH" is the environment variable that is read
        # the syntax means if RAW_BASE_PATH is set then read it since it's within the first comma,
        # if it's not set then use the path after the comma
    # "Path(...)" means to convert the string path that we just chose into an object for import os usage
    # .resolve() comverts the path into an absolute path; not relative path.

    ingest_date = os.getenv("INGEST_DATE", "").strip()
    # .strip() removes any whitespace from the beginning and end of the string
        # this includes spaces " ", tabs \t, and newlines \n
    if not ingest_date:
        ingest_date = date.today().isoformat()  # YYYY-MM-DD
        # date.today() returns today's date as an object
        # .isoformat() converts the date into a string format

    # Local source file (repo-stored). This replaces any internet download.
    ############################
    #####################
    ###############
    ###### Change this to take input of data file and process it in my project
    ###### Right now it is only taking "Nacimientos_HGM.xlsx" but I want it to eventually take what data file I input.
    source_xlsx_path = Path(os.getenv("SOURCE_XLSX_PATH", "data/milagros/Nacimientos_HGM.xlsx")).resolve()

    print(f"[milagros] RAW_BASE_PATH={raw_base_path}")
    # f"..." is a formatted string literal. 
        # Anything inside {} is evaluated as Python code and inserted into the string
        # In this example the {raw_base_path} inside the brackets {} is evaluated as the variable assigned... 
            # ...earlier at LINE # -> raw_base_path = Path(os.getenv("RAW_BASE_PATH", "data/raw/births")).resolve()

    print(f"[milgaros] INGEST_DATE={ingest_date}")
    print(f"[milagros] SOURCE_XLSX_PATH={source_xlsx_path}")

    # --- Output paths ---
    out_dir = raw_base_path / f"ingest_date={ingest_date}"
    out_dir.mkdir(parents=True, exist_ok=True)
        # ".mkdir" Make directory at this path
        # "parents=True" Create all missing parent directories, without this...
        # ...mkdir would fail unless all parent folders already exist
        # "exist_ok=True" If the directory already exists do nothing without this...
        # ... Python would raise "FileExistsError"

    success_marker = out_dir / "_SUCCESS"
    dest_file = out_dir / source_xlsx_path.name

    # --- Idempotency ---
    # Idempotency means: You can run the same operation multiple times and the result will be the same as running it once.
    # Plain example (non-technical)
    # Light switch
        # Turn it ON
        # Turn it ON again
        # Result: ON
        # Nothing changes the second time → idempotent
    # Non-idempotent example
        # Add $10 to your account
        # Add $10 again
        # Result changes each time → not idempotent
    # For my project Idempotency means:
        # First run → copy file + create _SUCCESS
        # Second run → skip, no duplication

    if success_marker.exists() and dest_file.exists():
        print(f"[milagros] Raw already present for {ingest_date}; skipping.")
        return

    # --- Validate source exists ---
    if not source_xlsx_path.exists():
        raise FileNotFoundError(f"Source XLSX not found: {source_xlsx_path}")

    # --- Copy local source into raw landing zone ---
    print(f"[milagros] Copying source → raw: {source_xlsx_path} → {dest_file}")
    shutil.copy2(source_xlsx_path, dest_file)
    # .copy only copies content and .copy2 copies content and metadata

    # Mark success
    success_marker.touch()
    print(f"[milagros] Wrote: {dest_file}")
    print(f"[milagros] Wrote: {success_marker}")



if __name__ == "__main__":
    main()

# This if__name__=="__main__": main() logis is important for Data Engineering...
# ...ensures main() runs only when the file is executed directly,...
# ...not when it’s imported by Airflow, tests, or other scripts.
# Every python file has a built-in variable called "__name__"...
    # ...and the value depends on how the file is used
    # If the file is run directly python sets __name__ = "__main__" and the condition is set to "True"
    # example of running file directly 
        # CL-> python ingest_births_hospital.py 
        # main() runs in this case
    # When the file is imported python sets __name__= "name of imported" and the condition is set to "False"
    # example of importing 
        # CL-> import ingest_births_hospitals
        # main() does not run



