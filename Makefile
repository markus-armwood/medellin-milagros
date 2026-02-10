PHONY: bootstrap test dq

bootstrap:
	python infra/bootstrap_local.py

test:
	pytest

dq:
	python src/quality/milagros/checks.py --ingest-date 2026-01-28