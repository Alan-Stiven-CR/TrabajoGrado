DROP TABLE IF EXISTS tabla_cuenta_ahorro_clientes PURGE;

CREATE TABLE tabla_cuenta_ahorro_clientes STORED AS PARQUET AS

SELECT
    num_doc
    ,cod_tipo_doc
    ,year
    ,month
    ,prom_cuenta_smmlv
    ,max_cuenta_smmlv
    ,min_cuenta_smmlv
    ,vdbal_stddev_sld_dia_mes_cta_aho_smmlv
    ,vdbal_coef_var_sld_mes_cta_aho_smmlv
FROM
   cuenta_ahorro_clientes
WHERE
    year IN (2025, 2024, 2023, 2022, 2021);

COMPUTE STATS tabla_cuenta_ahorro_clientes;
