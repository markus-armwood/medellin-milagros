from pathlib import Path

def test_raw_births_folder_exists():
    p = Path("raw/milagros/births")
    assert p.exists(), "Missing raw/milagros/births"



