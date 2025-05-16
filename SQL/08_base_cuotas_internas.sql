DROP TABLE IF EXISTS tabla_cuotas_financieras_internas_1 PURGE;

CREATE TABLE tabla_cuotas_financieras_internas_1 STORED AS PARQUET AS

WITH fecs AS (
    SELECT 
        year, 
        month, 
        max(day) as day 
    FROM 
        cuotas_financieras_internas
    WHERE
        year IN (2025, 2024, 2023, 2022, 2021)
    GROUP BY
        year, 
        month
)

,all_ci AS (
    SELECT 
        CAST(t1.num_doc_orig AS BIGINT) AS num_doc
        ,CAST(t1.tipo_num_doc AS INT) AS cod_tipo_doc
        ,t1.year
        ,t1.month
        ,CASE
            WHEN
                t1.valcta_credccial_ci IS NULL AND
                t1.valcta_credcmo_ci IS NULL AND
                t1.valcta_credhipo_ci IS NULL AND
                t1.valcta_credmicro_ci IS NULL AND
                t1.valcta_tdc_pesos_ci IS NULL
            THEN NULL    

            ELSE
                COALESCE(CAST(t1.valcta_credccial_ci AS FLOAT), 0) + 
                COALESCE(CAST(t1.valcta_credcmo_ci AS FLOAT), 0) + 
                COALESCE(CAST(t1.valcta_credhipo_ci AS FLOAT), 0) + 
                COALESCE(CAST(t1.valcta_credmicro_ci AS FLOAT), 0) + 
                COALESCE(CAST(t1.valcta_tdc_pesos_ci AS FLOAT), 0) 
        END AS cuota_total_ci
    FROM
        cuotas_financieras_internas AS t1
    INNER JOIN fecs USING (year, month, day)
)

SELECT 
    t1.num_doc
    ,t1.cod_tipo_doc
    ,t1.year
    ,t1.month
    ,(t1.cuota_total_ci / t2.salario) AS cuota_total_ci_smmlv
FROM 
    all_ci AS t1
LEFT JOIN
    salario_minimo_colombia AS t2
ON
    t1.year = t2.year;

COMPUTE STATS tabla_cuotas_financieras_internas_1;


DROP TABLE IF EXISTS tabla_cuotas_financieras_internas PURGE;

CREATE TABLE tabla_cuotas_financieras_internas STORED AS PARQUET AS

SELECT DISTINCT
    num_doc
    ,cod_tipo_doc
    ,year
    ,month
    ,cuota_total_ci_smmlv
FROM
    tabla_cuotas_financieras_internas_1;

COMPUTE STATS tabla_cuotas_financieras_internas;