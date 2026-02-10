from pathlib import Path

def test_infra_dirs_exist():
    assert Path("raw/milagros/births").exists()
    assert Path("processed/silver/milagros").exists()
    assert Path("processed/gold/milagros").exists()