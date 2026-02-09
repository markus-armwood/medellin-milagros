# Silver Contract — Milagros (HGM)

## Location
processed/silver/milagros/ingest_date=YYYY-MM-DD/

## Format
Parquet readable by Spark (no Parquet TIMESTAMP(NANOS)).

## Row meaning
One record per birth.

## Schema (strict)
- ano (bigint)
- periodo_de_reporte (bigint)
- sexo (string)
- peso_gramos (bigint)
- talla_centimetros (bigint)
- fecha_nacimiento (timestamp_ntz)
- parto_atendido_por (string)
- tiempo_de_gestacion (double)
- numero_consultas_prenatales (bigint)
- tipo_parto (string)
- multiplicidad_embarazo (string)
- apgar1 (double)
- apgar2 (double)
- grupo_sanguineo (string)
- factor_rh (string)
- pertenencia_etnica (string)
- edad_madre (bigint)
- estado_conyugal_madre (string)
- nivel_educativo_madre (string)
- ultimo_ano_aprobado_madre (double)
- pais_residencia (string)
- departamento_residencia (string)
- municipio_residencia (string)
- area_residencia (string)
- localidad (string)
- numero_hijos_nacidos_vivos (bigint)
- numero_embarazos (bigint)
- regimen_seguridad (string)
- nombre_administradora (string)
- edad_padre (bigint)
- nivel_educativo_padre (string)
- profesion_certificador (string)

## DQ rules enforced (Phase 5)
- Row count > 0
- Schema drift fails (missing/unexpected columns fail)
- Completeness (null allowance):
  - sexo, fecha_nacimiento, edad_madre, municipio_residencia
  - max null rate per field: 0.1%
- Domain:
  - sexo ∈ {FEMENINO, MASCULINO, INDETERMINADO} (trimmed)
- Range:
  - edad_madre ∈ [10, 60]