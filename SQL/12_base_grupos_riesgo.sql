DROP TABLE IF EXISTS tabla_grupos_riesgo PURGE;

CREATE TABLE tabla_grupos_riesgo STORED AS PARQUET AS

SELECT  
    year * 100 + month AS fecha_orden
    ,year
    ,month
    ,num_doc
    ,MAX(cod_tipo_doc) AS cod_tipo_doc
    ,MAX(cod_segm) AS cod_segm
    ,MAX(valor_g) AS valor_g
FROM
    grupos_riesgo
WHERE   
    year IN (2025, 2024, 2023, 2022, 2021)
    AND modelo_g = 'NUEVA_G'
GROUP BY
    year * 100 + month
    ,year
    ,month
    ,num_doc;

COMPUTE STATS tabla_grupos_riesgo;