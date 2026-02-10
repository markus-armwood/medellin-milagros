from pathlib import Path

def test_silver_milagros_exists():
    assert Path("processed/silver/milagros").exists(), "Missing processed/silver/milagros"