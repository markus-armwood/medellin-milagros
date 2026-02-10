# Births Dataset — Data Quality Contract

## Required Columns (NOT NULL)
- birth_date
- mother_age
- municipality
- ingest_date

## Valid Ranges
- mother_age: 10–60
- birth_date <= ingest_date

## Domain Constraints
## This is a set of all possible values
- sex ∈ {M, F}

## Uniqueness
## This is a tuple/ordered pair and we need BOTH together in this specific order to uniquely locate something.
- (birth_id, ingest_date)

## Schema
- birth_date: date
- mother_age: integer
- municipality: string
- ingest_date: date