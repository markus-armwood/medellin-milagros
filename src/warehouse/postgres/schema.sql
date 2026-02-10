CREATE SCHEMA IF NOT EXISTS milagros;

DROP TABLE IF EXISTS milagros.births_by_municipio;
CREATE TABLE milagros.births_by_municipio (
  departamento_residencia TEXT,
  municipio_residencia TEXT,
  births BIGINT
);

DROP TABLE IF EXISTS milagros.births_by_sexo;
CREATE TABLE milagros.births_by_sexo (
  sexo TEXT,
  births BIGINT
);

DROP TABLE IF EXISTS milagros.weight_summary;
CREATE TABLE milagros.weight_summary (
  n BIGINT,
  avg_peso_gramos DOUBLE PRECISION,
  median_peso_gramos DOUBLE PRECISION,
  min_peso_gramos DOUBLE PRECISION,
  max_peso_gramos DOUBLE PRECISION
);

DROP TABLE IF EXISTS milagros.mother_age_bands;
CREATE TABLE milagros.mother_age_bands (
  mother_age_band TEXT,
  births BIGINT
);

DROP TABLE IF EXISTS milagros.father_age_bands;
CREATE TABLE milagros.father_age_bands (
  father_age_band TEXT,
  births BIGINT
);