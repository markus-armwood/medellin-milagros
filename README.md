# Medellín Milagros — Data Engineering Project

End-to-end Data Engineering pipeline built on Colombian hospital birth data.

This project demonstrates a production-style pipeline covering ingestion, transformation, analytics modeling, data quality, testing, and infrastructure automation.

---

## Architecture

Raw → Silver → Gold → Data Quality → Tests → Infra
(local-first, production-style pipeline)

- **Raw**: local landing zone for birth datasets  
- **Silver**: cleaned + typed canonical tables  
- **Gold**: analytics-ready aggregates

### Layers

- **Raw**  
  Local landing zone for birth records  
  `raw/milagros/births`

- **Silver**  
  Cleaned + typed Parquet  
  `processed/silver/milagros/ingest_date=YYYY-MM-DD`

- **Gold**  
  Analytics-ready datasets  
  `processed/gold/milagros/ingest_date=YYYY-MM-DD`

Gold tables produced:

- births_by_municipio  
- births_by_sexo  
- weight_summary  
- mother_age_bands  
- father_age_bands  

---

## Tech Stack

- Python 3
- PySpark
- Parquet
- pytest
- Make
- GitHub + GitHub Actions

(Local-first design with cloud-ready structure.)

---

## Project Structure

medellin-milagros/
├── raw/
│   └── milagros/
│       └── births/
├── processed/
│   ├── silver/
│   │   └── milagros/
│   │       └── ingest_date=YYYY-MM-DD/
│   └── gold/
│       └── milagros/
│           └── ingest_date=YYYY-MM-DD/
├── src/
│   ├── ingestion/
│   ├── silver/
│   ├── gold/
│   │   └── milagros/
│   │       └── gold.py
│   └── quality/
│       └── milagros/
│           └── checks.py
├── tests/
├── infra/
│   └── bootstrap_local.py
├── artifacts/
├── Makefile
└── README.md

---

## Quickstart

From repo root:

```bash
make bootstrap
pytest
python src/gold/milagros/gold.py --ingest-date 2026-01-28
python src/quality/milagros/checks.py --ingest-date 2026-01-28