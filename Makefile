PHONY: bootstrap test dq

bootstrap:
	python infra/bootstrap_local.py

test:
	pytest

dq:
	python src/quality/milagros/checks.py --ingest-date 2026-01-28

pg-up:
	docker compose up -d

pg-down:
	docker compose down

pg-psql:
	psql -h localhost -p 5433 -U milagros -d milagros

pg-load:
	python src/warehouse/load_postgres.py --ingest-date 2026-01-28

pg-psql:
	psql -h localhost -p 5433 -U milagros -d milagros
	#password: milagros