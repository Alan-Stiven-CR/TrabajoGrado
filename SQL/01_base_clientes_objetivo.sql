-- MCD --
DROP TABLE IF EXISTS tabla_clientes PURGE;

CREATE TABLE tabla_clientes STORED AS PARQUET AS

WITH corte_mcd AS (
    SELECT
        year,
        month,
        MAX(day) AS max_ingestion_day
    FROM 
        Clientes
    WHERE 
        year IN (2025, 2024, 2023, 2022, 2021)
    GROUP BY 
        year,
        month
    ORDER BY 
        year DESC, 
        month DESC
)

SELECT
    mcd.year
    ,mcd.month
    ,mcd.cod_tipo_cli
    ,mcd.tipo_cli
    ,mcd.cod_tipo_doc
    ,CASE
        WHEN TRIM(mcd.cod_estado_cli) = '1' THEN 1
        ELSE 0
    END AS cod_estado_cli
    ,mcd.num_doc
    ,mcd.cod_segm
FROM 
    resultados_vspc_clientes.master_customer_data AS mcd
INNER JOIN 
    corte_mcd AS corte
    ON mcd.year = corte.year
    AND  mcd.month = corte.month

WHERE 
    TRIM(UPPER(mcd.cod_segm)) IN ('4','6','9','M','S') AND
    TRIM(UPPER(mcd.tipo_cli)) = 'PERSONA NATURAL' AND
    TRIM(UPPER(mcd.cod_estado_cli)) = '1';

COMPUTE STATS tabla_clientes;



DROP TABLE IF EXISTS tabla_clientes_2 PURGE;

CREATE TABLE tabla_clientes_2 STORED AS PARQUET AS
SELECT DISTINCT
    year
    ,month
    ,cod_tipo_cli
    ,tipo_cli
    ,cod_tipo_doc
    ,cod_estado_cli
    ,num_doc
    ,cod_segm
FROM
    tabla_clientes;

COMPUTE STATS tabla_clientes_2;



-- MCR --
DROP TABLE IF EXISTS tabla_riesgo_credito PURGE;

CREATE TABLE tabla_riesgo_credito STORED AS PARQUET AS
WITH corte_mcr AS (
    SELECT
        year,
        month
        FROM 
            riesgo_credito
        WHERE 
            year IN (2025, 2024, 2023, 2022, 2021)
        GROUP BY 
            year,
            month
        ORDER BY 
            year DESC, 
            month DESC
)
SELECT DISTINCT
    mc.year
    ,mc.month
    ,CAST(mc.tipo_identificacion_cli AS INT) AS tipo_doc_mcr
    ,CAST(mc.num_doc AS BIGINT) AS num_doc_mcr
    ,mc.f_corte
    ,mc.f_desemb
    ,mc.dias_mora
FROM 
    riesgo_credito AS mc
INNER JOIN
    corte_mcr
    USING (year, month)
WHERE
    mc.libro = 1;

COMPUTE STATS tabla_riesgo_credito;



-- Base clientes objetivo proyecto interpretedores gestion de cupo y Score --

DROP TABLE IF EXISTS tabla_riesgo_credito_2 PURGE;

CREATE TABLE tabla_riesgo_credito_2 STORED AS PARQUET AS
SELECT
    mcd.year
    ,mcd.month
    ,CAST(mcd.cod_tipo_doc AS INT) AS tipo_doc
    ,mcd.cod_estado_cli
    ,CAST(mcd.num_doc AS BIGINT) AS num_doc
    ,mcd.cod_segm
    ,mcr.dias_mora
FROM 
    tabla_clientes_2 AS mcd
LEFT JOIN
    tabla_riesgo_credito AS mcr
    ON CAST(mcd.cod_tipo_doc AS INT) = mcr.tipo_doc_mcr
    AND CAST(mcd.num_doc AS BIGINT) = mcr.num_doc_mcr
    AND mcd.year = mcr.year
    AND mcd.month = mcr.month
WHERE
    mcd.cod_estado_cli IN (1);

COMPUTE STATS tabla_riesgo_credito_2;


DROP TABLE IF EXISTS tabla_clientes_objetivo PURGE;

CREATE TABLE tabla_clientes_objetivo STORED AS PARQUET AS

SELECT
    year,
    month,
    MAX(tipo_doc) AS tipo_doc,
    cod_estado_cli,
    num_doc,
    MAX(cod_segm) AS cod_segm,
    MAX(dias_mora) AS dias_mora
FROM
    tabla_riesgo_credito_2
GROUP BY
    year,
    month,
    cod_estado_cli,
    num_doc;

COMPUTE STATS tabla_clientes_objetivo;
