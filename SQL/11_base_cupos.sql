DROP TABLE IF EXISTS tabla_cupos_1 PURGE;

CREATE TABLE tabla_cupos_1 STORED AS PARQUET AS

SELECT
    t1.num_doc,
    t1.cod_tipo_doc,
    t1.num_cta,
    APPX_MEDIAN(t1.cupo) AS cupo,
    t1.year,
    t1.month
FROM cupos as t1
WHERE t1.desc_estado_vigencia = 'Vigente'
        AND t1.tdc_amparada = 'NO'
        AND t1.tipo_tdc = 'PERSONA'
        AND  year IN (2025, 2024, 2023, 2022, 2021)
GROUP BY
    t1.num_doc,
    t1.cod_tipo_doc,
    t1.num_cta,
    t1.year,
    t1.month;

COMPUTE STATS tabla_cupos_1;


DROP TABLE IF EXISTS tabla_compras PURGE;

CREATE TABLE tabla_compras STORED AS PARQUET AS

SELECT  
    t1.num_cta,
    t1.year,
    t1.month,
    SUM(t1.val_trx_cop) AS compras_cliente
FROM 
    consumo_tarjetas AS t1
WHERE
    t1.year IN (2025, 2024, 2023, 2022, 2021)
    AND num_cuotas = 1
    AND TRIM(cat_tripa) IN ('COMPRA')
    AND TRIM(grp_trx) IN ('COMPRA')
GROUP BY
    t1.num_cta,
    t1.year,
    t1.month;

COMPUTE STATS tabla_compras;


DROP TABLE IF EXISTS tabla_avances PURGE;

CREATE TABLE tabla_avances STORED AS PARQUET AS

SELECT  
    t1.num_cta,
    t1.year,
    t1.month,
    SUM(t1.val_trx_cop) AS avances
FROM 
    consumo_tarjetas AS t1
WHERE
    t1.year IN (2025, 2024, 2023, 2022, 2021)
    AND TRIM(cat_tripa) IN ('AVANCE')
    AND TRIM(grp_trx) IN ('AVANCE')
GROUP BY
    t1.num_cta,
    t1.year,
    t1.month;

COMPUTE STATS tabla_avances;

DROP TABLE IF EXISTS tabla_unida_cupos_compras PURGE;

CREATE TABLE tabla_unida_cupos_compras STORED AS PARQUET AS

SELECT
    t1.num_doc,
    t1.cod_tipo_doc,
    t1.num_cta,
    t1.cupo,
    t2.compras_cliente,
    t3.avances,
    t4.valor_ingreso_estimadores_smmlv,
    t1.year,
    t1.month
FROM
    tabla_cupos_1 AS t1
LEFT JOIN
    tabla_compras AS t2
ON 
    t1.num_cta = t2.num_cta
    AND t1.year = t2.year
    AND t1.month = t2.month
LEFT JOIN
    tabla_avances AS t3
ON
    t1.num_cta = t3.num_cta
    AND t1.year = t3.year
    AND t1.month = t3.month
LEFT JOIN
    tabla_estimadores_ingresos AS t4
ON
    CAST(t1.num_doc AS BIGINT) = t4.num_doc
    AND CAST(t1.cod_tipo_doc AS INT) = t4.cod_tipo_doc
    AND t1.year = t4.ingestion_year
    AND t1.month = t4.ingestion_month;

COMPUTE STATS tabla_unida_cupos_compras;


DROP TABLE IF EXISTS tabla_cupos PURGE;

CREATE TABLE tabla_cupos STORED AS PARQUET AS 

WITH base_smmlv AS (
SELECT
    t1.num_doc,
    t1.cod_tipo_doc,
    t1.num_cta,
    t1.cupo,
    (t1.cupo / t2.salario) AS cupo_smmlv,
    (t1.compras_cliente / t2.salario) AS compras_cliente_smmlv,
    (t1.avances / t2.salario) AS avances_smmlv,
    t1.valor_ingreso_estimadores_smmlv,
    t1.year,
    t1.month
FROM
    tabla_unida_cupos_compras AS t1
LEFT JOIN
    salario_minimo_colombia AS t2
    ON 
        t1.year = t2.year)

SELECT
    num_doc,
    cod_tipo_doc,
    num_cta,
    cupo,
    cupo_smmlv,
    compras_cliente_smmlv,
    avances_smmlv,
    valor_ingreso_estimadores_smmlv,
    compras_cliente_smmlv / cupo_smmlv AS razon_compras_cupo,
    avances_smmlv / cupo_smmlv AS razon_avances_cupo,
    compras_cliente_smmlv / valor_ingreso_estimadores_smmlv AS razon_compra_ingreso_estimadores,
    year,
    month
FROM
    base_smmlv;

COMPUTE STATS tabla_cupos;