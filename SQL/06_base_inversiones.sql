DROP TABLE IF EXISTS tabla_inversiones PURGE;

CREATE TABLE tabla_inversiones STORED AS PARQUET AS

SELECT
    num_doc,
    MAX(cod_tipo_doc) AS cod_tipo_doc,
    MAX(flag_inversiones) AS flag_inversiones,
    MAX(cnt_total_inversiones) AS cnt_total_inversiones,
    month AS month,
    year AS year

FROM
    inversiones
WHERE
    year IN (2025, 2024, 2023, 2022, 2021)
GROUP BY
    num_doc,
    month,
    year;

COMPUTE STATS tabla_inversiones;