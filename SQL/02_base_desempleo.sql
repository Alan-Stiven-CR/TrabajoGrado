DROP TABLE IF EXISTS tabla_desempleo PURGE;

CREATE TABLE tabla_desempleo STORED AS PARQUET AS

SELECT
    num_doc,
    cod_tipo_doc,
    year,
    month,
    MAX(desempleado) AS desempleado 
FROM
   seguimiento_desempleo
WHERE
    year IN (2025, 2024, 2023, 2022, 2021)
GROUP BY
    num_doc,
    cod_tipo_doc,
    year,
    month;

COMPUTE STATS tabla_desempleo;
