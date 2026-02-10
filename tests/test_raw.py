from pathlib import Path

def test_raw_births_exists():
    assert Path("raw/milagros/births").exists(), "Missing raw/milagros/births"