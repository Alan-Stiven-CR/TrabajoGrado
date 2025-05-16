DROP TABLE IF EXISTS tabla_estimadores_ingresos_1 PURGE;

CREATE TABLE tabla_estimadores_ingresos_1 STORED AS PARQUET AS
SELECT
    year
    ,month
    ,MAX(cod_tipo_doc) AS cod_tipo_doc
    ,num_doc
    ,SUM(valor_ingreso) AS valor_ingreso
FROM
    estimadores_ingresos
WHERE
    year >= 2021 AND
    modelo_fuente = 'INGRESO_FINAL'
GROUP BY
    year
    ,month
    ,num_doc;

COMPUTE STATS tabla_estimadores_ingresos_1;

DROP TABLE IF EXISTS tabla_estimadores_ingresos PURGE;

CREATE TABLE tabla_estimadores_ingresos STORED AS PARQUET AS
SELECT
    t1.year
    ,t1.month
    ,MAX(t1.cod_tipo_doc) AS cod_tipo_doc
    ,t1.num_doc
    ,SUM(t1.valor_ingreso/t2.salario) AS valor_ingreso_estimadores_smmlv
FROM
    tabla_estimadores_ingresos_1 AS t1
LEFT JOIN
    salario_minimo_colombia AS t2
ON
    t1.year = t2.year
GROUP BY
    t1.year
    ,t1.month
    ,t1.num_doc;

COMPUTE STATS tabla_estimadores_ingresos;