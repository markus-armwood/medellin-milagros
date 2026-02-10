from pathlib import Path

def test_gold_outputs_exist_for_ingest_date():
    base = Path("processed/gold/milagros/ingest_date=2026-01-28")
    assert base.exists(), "Missing Gold partition folder"

    assert (base / "births_by_municipio").exists(), "Missing births_by_municipio"
    assert (base / "births_by_sexo").exists(), "Missing births_by_sexo"
    assert (base / "weight_summary").exists(), "Missing weight_summary"
    assert (base / "_SUCCESS").exists(), "Missing _SUCCESS marker"