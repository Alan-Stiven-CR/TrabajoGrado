DROP TABLE IF EXISTS tabla_cuotas_financieras_externas_1 PURGE;

CREATE TABLE tabla_cuotas_financieras_externas_1 STORED AS PARQUET AS

WITH fecs AS (
    SELECT 
        year, 
        month, 
        max(day) as day 
    FROM 
        cuotas_financieras_externas
    WHERE
        year IN (2025, 2024, 2023, 2022, 2021)
    GROUP BY
        year, 
        month
)

,all_ce AS (
    SELECT 
        CAST(t1.num_doc AS INT) AS num_doc
        ,CAST(t1.tipo_num_doc AS INT) AS cod_tipo_doc
        ,t1.year
        ,t1.month
        ,CASE
            WHEN
                t1.cuota_tot_prod_hip_ce IS NULL AND
                t1.cuota_tot_prod_instal_ce IS NULL AND
                t1.cuota_tot_prod_rota_ce IS NULL AND
                t1.cuota_tot_prod_vehi_ce IS NULL AND
                t1.pago_mes_tdc_ppal_ce IS NULL
            THEN NULL    

            ELSE
                (COALESCE(CAST(t1.cuota_tot_prod_hip_ce AS FLOAT), 0) + 
                COALESCE(CAST(t1.cuota_tot_prod_instal_ce AS FLOAT), 0) + 
                COALESCE(CAST(t1.cuota_tot_prod_rota_ce AS FLOAT), 0) + 
                COALESCE(CAST(t1.cuota_tot_prod_vehi_ce AS FLOAT), 0) + 
                COALESCE(CAST(t1.pago_mes_tdc_ppal_ce AS FLOAT), 0))*1000 
        END AS cuota_total_ce
    FROM
        cuotas_financieras_externas AS t1
    INNER JOIN fecs USING (year, month, day)
)

SELECT 
    t1.num_doc
    ,t1.cod_tipo_doc
    ,t1.year
    ,t1.month
    ,(t1.cuota_total_ce / t2.salario) AS cuota_total_ce_smmlv 
FROM 
    all_ce AS t1
LEFT JOIN
    salario_minimo_colombia AS t2
ON
    t1.year = t2.year
;

COMPUTE STATS tabla_cuotas_financieras_externas_1;


DROP TABLE IF EXISTS tabla_cuotas_financieras_externas PURGE;

CREATE TABLE tabla_cuotas_financieras_externas STORED AS PARQUET AS

SELECT DISTINCT
    t1.num_doc
    ,t1.cod_tipo_doc
    ,t1.year
    ,t1.month
    ,t1.cuota_total_ce_smmlv
FROM
    tabla_cuotas_financieras_externas_1 AS t1;

COMPUTE STATS tabla_cuotas_financieras_externas;