DROP TABLE IF EXISTS tabla_cruce_desempleo PURGE;

CREATE TABLE tabla_cruce_desempleo STORED AS PARQUET AS

SELECT
    t1.year
    ,t1.month
    ,t1.cod_tipo_doc
    ,t1.cod_estado_cli
    ,t1.num_doc
    ,t1.cod_segm
    ,t1.dias_mora
    ,t2.desempleado
FROM
    tabla_clientes_objetivo AS t1
LEFT JOIN
    tabla_desempleo AS t2
ON
    t1.num_doc = t2.num_doc AND
    t1.cod_tipo_doc = t2.cod_tipo_doc AND
    t1.year = t2.year AND
    t1.month = t2.month;

COMPUTE STATS tabla_cruce_desempleo;


DROP TABLE IF EXISTS tabla_cruce_ingresos PURGE;

CREATE TABLE tabla_cruce_ingresos STORED AS PARQUET AS
SELECT
    t1.year
    ,t1.month
    ,t1.cod_tipo_doc
    ,t1.cod_estado_cli
    ,t1.num_doc
    ,t1.cod_segm
    ,t1.dias_mora
    ,t1.desempleado
    ,CASE
        WHEN t2.valor_ingreso_total_pic_smmlv IS NOT NULL THEN t2.valor_ingreso_total_pic_smmlv
        WHEN t1.cod_segm IN ('S', '4') AND t2.ingreso_ss_smmlv IS NOT NULL THEN t2.ingreso_ss_smmlv
        WHEN t2.mnt_trx_fijos_smmlv IS NOT NULL THEN t2.mnt_trx_fijos_smmlv
        ELSE 0
    END AS ingreso_final_smmlv
    ,CASE
        WHEN t2.valor_ingreso_total_pic_smmlv IS NOT NULL THEN "pic"
        WHEN t1.cod_segm IN ('S', '4') AND t2.ingreso_ss_smmlv IS NOT NULL THEN "ss"
        WHEN t2.mnt_trx_fijos_smmlv IS NOT NULL THEN "fijo"
        ELSE "no_definido"
    END AS fuente_ingreso_final_smmlv
    ,t2.mnt_trx_var_smmlv
    ,t3.valor_ingreso_estimadores_smmlv
FROM
    tabla_cruce_desempleo AS t1
LEFT JOIN
    tabla_ingresos_personales AS t2
ON
    t1.num_doc = t2.num_doc AND
    t1.cod_tipo_doc = t2.cod_tipo_doc AND
    t1.year = t2.year AND
    t1.month = t2.month
LEFT JOIN
    tabla_estimadores_ingresos AS t3
    ON
        t1.num_doc = t3.num_doc AND
        t1.cod_tipo_doc = t3.cod_tipo_doc AND
        t1.year = t3.year AND
        t1.month = t3.month;

COMPUTE STATS tabla_cruce_ingresos;


DROP TABLE IF EXISTS tabla_cruce_ahorro PURGE;

CREATE TABLE tabla_cruce_ahorro STORED AS PARQUET AS
SELECT
    t1.year
    ,t1.month
    ,t1.cod_tipo_doc
    ,t1.cod_estado_cli
    ,t1.num_doc
    ,t1.cod_segm
    ,t1.dias_mora
    ,t1.desempleado
    ,t1.ingreso_final_smmlv
    ,t1.fuente_ingreso_final_smmlv
    ,t1.mnt_trx_var_smmlv
    ,t1.valor_ingreso_estimadores_smmlv
    ,t2.prom_cuenta_smmlv
    ,t2.max_cuenta_smmlv
    ,t2.min_cuenta_smmlv
    ,t2.vdbal_stddev_sld_dia_mes_cta_aho_smmlv
    ,t2.vdbal_coef_var_sld_mes_cta_aho_smmlv
FROM
    tabla_cruce_ingresos AS t1
LEFT JOIN
    tabla_cuenta_ahorro_clientes AS t2
ON
    t1.num_doc = t2.num_doc AND
    t1.cod_tipo_doc = t2.cod_tipo_doc AND
    t1.year = t2.year AND
    t1.month = t2.month;

COMPUTE STATS tabla_cruce_ahorro;


DROP TABLE IF EXISTS tabla_cruce_basicas_no_basicas PURGE;

CREATE TABLE tabla_cruce_basicas_no_basicas STORED AS PARQUET AS
SELECT
    t1.year
    ,t1.month
    ,t1.cod_tipo_doc
    ,t1.cod_estado_cli
    ,t1.num_doc
    ,t1.cod_segm
    ,t1.dias_mora
    ,t1.desempleado
    ,t1.ingreso_final_smmlv
    ,t1.fuente_ingreso_final_smmlv
    ,t1.mnt_trx_var_smmlv
    ,t1.valor_ingreso_estimadores_smmlv
    ,t1.prom_cuenta_smmlv
    ,t1.max_cuenta_smmlv
    ,t1.min_cuenta_smmlv
    ,t1.vdbal_stddev_sld_dia_mes_cta_aho_smmlv
    ,t1.vdbal_coef_var_sld_mes_cta_aho_smmlv
    ,t2.compras_basicas
    ,t2.compras_no_basicas
    ,CASE 
        WHEN t2.compras_basicas IS NULL OR t2.compras_no_basicas IS NULL THEN NULL
        WHEN (t2.compras_basicas + t2.compras_no_basicas) = 0 THEN NULL
        ELSE t2.compras_no_basicas / ((t2.compras_basicas) + t2.compras_no_basicas)
    END AS razon_compras_no_basicas
    ,CASE 
        WHEN t2.compras_basicas IS NULL OR t2.compras_no_basicas IS NULL THEN NULL
        WHEN (t2.compras_basicas + t2.compras_no_basicas) = 0 THEN NULL
        ELSE t2.compras_basicas / ((t2.compras_basicas) + t2.compras_no_basicas)
    END AS razon_compras_basicas
FROM
   tabla_cruce_ahorro AS t1 
LEFT JOIN
    tabla_modulos_subcategorias AS t2
ON
    t1.num_doc = t2.num_doc AND
    t1.cod_tipo_doc = t2.cod_tipo_doc AND
    t1.year = t2.year AND
    t1.month = t2.month;

COMPUTE STATS tabla_cruce_basicas_no_basicas;


DROP TABLE IF EXISTS tabla_cruce_inversiones PURGE;

CREATE TABLE tabla_cruce_inversiones STORED AS PARQUET AS
SELECT
    t1.year
    ,t1.month
    ,t1.cod_tipo_doc
    ,t1.cod_estado_cli
    ,t1.num_doc
    ,t1.cod_segm
    ,t1.dias_mora
    ,t1.desempleado
    ,t1.ingreso_final_smmlv
    ,t1.fuente_ingreso_final_smmlv
    ,t1.mnt_trx_var_smmlv
    ,t1.valor_ingreso_estimadores_smmlv
    ,t1.prom_cuenta_smmlv
    ,t1.max_cuenta_smmlv
    ,t1.min_cuenta_smmlv
    ,t1.vdbal_stddev_sld_dia_mes_cta_aho_smmlv
    ,t1.vdbal_coef_var_sld_mes_cta_aho_smmlv
    ,t1.compras_basicas
    ,t1.compras_no_basicas
    ,t1.razon_compras_basicas
    ,t1.razon_compras_no_basicas
    ,IF(razon_compras_basicas > 0.66, 1, 0 ) AS flag_compras_basicas
    ,IF(razon_compras_no_basicas > 0.66, 1, 0 ) AS flag_compras_no_basicas
    ,t2.flag_inversiones
    ,t2.cnt_total_inversiones
FROM
    tabla_cruce_basicas_no_basicas AS t1
LEFT JOIN 
    tabla_inversiones AS t2
ON
    t1.num_doc = CAST(t2.num_doc AS BIGINT) AND
    t1.cod_tipo_doc = CAST(t2.cod_tipo_doc AS INT) AND
    t1.year = t2.year AND
    t1.month = t2.month;

COMPUTE STATS tabla_cruce_inversiones;


DROP TABLE IF EXISTS tabla_cruce_cuotas_financieras PURGE;

CREATE TABLE tabla_cruce_cuotas_financieras STORED AS PARQUET AS
SELECT
    t1.year
    ,t1.month
    ,t1.cod_tipo_doc
    ,t1.cod_estado_cli
    ,t1.num_doc
    ,t1.cod_segm
    ,t1.dias_mora
    ,t1.desempleado
    ,t1.ingreso_final_smmlv
    ,t1.fuente_ingreso_final_smmlv
    ,t1.mnt_trx_var_smmlv
    ,t1.valor_ingreso_estimadores_smmlv
    ,t1.prom_cuenta_smmlv
    ,t1.max_cuenta_smmlv
    ,t1.min_cuenta_smmlv
    ,t1.vdbal_stddev_sld_dia_mes_cta_aho_smmlv
    ,t1.vdbal_coef_var_sld_mes_cta_aho_smmlv
    ,t1.compras_basicas
    ,t1.compras_no_basicas
    ,t1.razon_compras_basicas
    ,t1.razon_compras_no_basicas
    ,t1.flag_compras_basicas
    ,t1.flag_compras_no_basicas
    ,t1.flag_inversiones
    ,t1.cnt_total_inversiones
    ,t2.cuota_total_ce_smmlv
    ,t3.cuota_total_ci_smmlv
FROM
    tabla_cruce_inversiones AS t1
LEFT JOIN
    tabla_cuotas_financieras_externas AS t2
ON
    t1.num_doc = CAST(t2.num_doc AS BIGINT) AND
    t1.cod_tipo_doc = CAST(t2.cod_tipo_doc AS INT) AND
    t1.year = t2.year AND
    t1.month = t2.month
LEFT JOIN
    tabla_cuotas_financieras_internas AS t3
ON
    t1.num_doc = CAST(t3.num_doc AS BIGINT) AND
    t1.cod_tipo_doc = CAST(t3.cod_tipo_doc AS INT) AND
    t1.year = t3.year AND
    t1.month = t3.month;

COMPUTE STATS tabla_cruce_cuotas_financieras;


DROP TABLE IF EXISTS tabla_cruce_categorias PURGE;

CREATE TABLE tabla_cruce_categorias STORED AS PARQUET AS
SELECT
    t1.year
    ,t1.month
    ,t1.cod_tipo_doc
    ,t1.cod_estado_cli
    ,t1.num_doc
    ,t1.cod_segm
    ,t1.dias_mora
    ,t1.desempleado
    ,t1.ingreso_final_smmlv
    ,t1.fuente_ingreso_final_smmlv
    ,t1.mnt_trx_var_smmlv
    ,t1.valor_ingreso_estimadores_smmlv
    ,t1.prom_cuenta_smmlv
    ,t1.max_cuenta_smmlv
    ,t1.min_cuenta_smmlv
    ,t1.vdbal_stddev_sld_dia_mes_cta_aho_smmlv
    ,t1.vdbal_coef_var_sld_mes_cta_aho_smmlv
    ,t1.compras_basicas
    ,t1.compras_no_basicas
    ,t1.razon_compras_basicas
    ,t1.razon_compras_no_basicas
    ,t1.flag_compras_basicas
    ,t1.flag_compras_no_basicas
    ,t1.flag_inversiones
    ,t1.cnt_total_inversiones
    ,t1.cuota_total_ce_smmlv
    ,t1.cuota_total_ci_smmlv
    ,t2.mnt_trx_cat_tdc
    ,t2.mnt_trx_cat_tdd 
    ,t2.mnt_trx_cat_retiros
    ,t2.gasto_total
FROM
    tabla_cruce_cuotas_financieras AS t1
LEFT JOIN
    tabla_modulos_categorias AS t2
ON
    t1.num_doc = CAST(t2.num_doc AS BIGINT) AND
    t1.cod_tipo_doc = CAST(t2.cod_tipo_doc AS INT) AND
    t1.year = t2.year AND
    t1.month = t2.month;

COMPUTE STATS tabla_cruce_categorias;


DROP TABLE IF EXISTS tabla_sobreendeudamiento PURGE;

CREATE TABLE tabla_sobreendeudamiento STORED AS PARQUET AS
WITH b_sobreen AS (

SELECT
    t1.year
    ,t1.month
    ,t1.cod_tipo_doc
    ,t1.cod_estado_cli
    ,t1.num_doc
    ,t1.cod_segm
    ,t1.dias_mora
    ,t1.desempleado
    ,t1.ingreso_final_smmlv
    ,t1.fuente_ingreso_final_smmlv
    ,t1.mnt_trx_var_smmlv
    ,t1.valor_ingreso_estimadores_smmlv
    ,t1.prom_cuenta_smmlv
    ,t1.max_cuenta_smmlv
    ,t1.min_cuenta_smmlv
    ,t1.vdbal_stddev_sld_dia_mes_cta_aho_smmlv
    ,t1.vdbal_coef_var_sld_mes_cta_aho_smmlv
    ,t1.compras_basicas
    ,t1.compras_no_basicas
    ,t1.razon_compras_basicas
    ,t1.razon_compras_no_basicas
    ,t1.flag_compras_basicas
    ,t1.flag_compras_no_basicas
    ,t1.flag_inversiones
    ,t1.cnt_total_inversiones
    ,t1.cuota_total_ce_smmlv
    ,t1.cuota_total_ci_smmlv
    ,t1.mnt_trx_cat_tdc
    ,t1.mnt_trx_cat_tdd 
    ,t1.mnt_trx_cat_retiros
    ,t1.gasto_total
    ,CASE
        WHEN ingreso_final_smmlv IS NULL OR ingreso_final_smmlv = 0 THEN NULL
        ELSE (t1.cuota_total_ce_smmlv / (t1.ingreso_final_smmlv))
    END AS razon_sobreendeudamiento_ce
    ,CASE 
        WHEN t1.ingreso_final_smmlv IS NULL OR t1.ingreso_final_smmlv = 0 THEN NULL
        WHEN t1.fuente_ingreso_final_smmlv = 'pic' OR t1.fuente_ingreso_final_smmlv = 'ss' THEN ((t1.cuota_total_ce_smmlv + t1.cuota_total_ci_smmlv) / (t1.ingreso_final_smmlv))
        WHEN t1.fuente_ingreso_final_smmlv = 'fijo' THEN (t1.cuota_total_ci_smmlv / (t1.ingreso_final_smmlv))
    END AS regla_razon_sobreendeudamiento
    ,CASE
        WHEN t1.ingreso_final_smmlv IS NULL OR t1.ingreso_final_smmlv = 0 THEN NULL
        ELSE (t1.cuota_total_ci_smmlv / (t1.ingreso_final_smmlv))
    END AS razon_sobreendeudamiento_ci
    ,CASE
        WHEN t1.ingreso_final_smmlv IS NULL OR t1.ingreso_final_smmlv = 0 THEN NULL
        ELSE ((t1.cuota_total_ce_smmlv + t1.cuota_total_ci_smmlv) / (t1.ingreso_final_smmlv))
    END AS razon_sobreendeudamiento_total
    ,CASE
        WHEN t1.valor_ingreso_estimadores_smmlv IS NULL OR t1.valor_ingreso_estimadores_smmlv = 0 THEN NULL
        ELSE (t1.cuota_total_ce_smmlv / (t1.valor_ingreso_estimadores_smmlv))
    END AS razon_sobreendeudamiento_ce_ingreso_estimadores
    ,CASE
        WHEN t1.valor_ingreso_estimadores_smmlv IS NULL OR t1.valor_ingreso_estimadores_smmlv = 0 THEN NULL
        ELSE (t1.cuota_total_ci_smmlv / (t1.valor_ingreso_estimadores_smmlv))
    END AS razon_sobreendeudamiento_ci_ingreso_estimadores
    ,CASE
        WHEN t1.valor_ingreso_estimadores_smmlv IS NULL OR t1.valor_ingreso_estimadores_smmlv = 0 THEN NULL
        ELSE ((t1.cuota_total_ce_smmlv + t1.cuota_total_ci_smmlv) / (t1.valor_ingreso_estimadores_smmlv))
    END AS razon_sobreendeudamiento_total_ingreso_estimadores
    ,CASE
        WHEN t1.ingreso_final_smmlv IS NULL OR t1.ingreso_final_smmlv = 0 THEN NULL
        WHEN t1.compras_basicas IS NULL
            THEN (1 - (t1.cuota_total_ce_smmlv) / (t1.ingreso_final_smmlv)) 
        WHEN t1.cuota_total_ce_smmlv IS NULL
            THEN (1 - (t1.compras_basicas) / (t1.ingreso_final_smmlv)) 
        ELSE
            (1 - (t1.compras_basicas + t1.cuota_total_ce_smmlv) / (t1.ingreso_final_smmlv)) 
    END AS capacidad_pago_ce
    
    ,CASE
        WHEN t1.ingreso_final_smmlv IS NULL OR t1.ingreso_final_smmlv = 0 THEN NULL
        WHEN t1.compras_basicas IS NULL
            THEN (1 - (t1.cuota_total_ci_smmlv) / (t1.ingreso_final_smmlv)) 
        WHEN t1.cuota_total_ci_smmlv IS NULL
            THEN (1 - (t1.compras_basicas) / (t1.ingreso_final_smmlv)) 
        ELSE
            (1 - (t1.compras_basicas + t1.cuota_total_ci_smmlv) / (t1.ingreso_final_smmlv)) 
    END AS capacidad_pago_ci

    ,CASE
        WHEN t1.ingreso_final_smmlv IS NULL OR t1.ingreso_final_smmlv = 0 THEN NULL
        WHEN t1.compras_basicas IS NULL
            THEN (1 - (t1.cuota_total_ci_smmlv + t1.cuota_total_ce_smmlv) / (t1.ingreso_final_smmlv)) 
        WHEN t1.cuota_total_ci_smmlv IS NULL
            THEN (1 - (t1.compras_basicas + t1.cuota_total_ce_smmlv) / (t1.ingreso_final_smmlv)) 
        WHEN t1.cuota_total_ce_smmlv IS NULL
            THEN (1 - (t1.compras_basicas + t1.cuota_total_ci_smmlv) / (t1.ingreso_final_smmlv))
        ELSE
            (1 - (t1.compras_basicas + t1.cuota_total_ce_smmlv + t1.cuota_total_ci_smmlv) / (t1.ingreso_final_smmlv))
    END AS capacidad_pago_total
    ,CASE
        WHEN t1.ingreso_final_smmlv IS NULL OR t1.ingreso_final_smmlv = 0 THEN NULL
        WHEN t1.fuente_ingreso_final_smmlv = 'pic' OR t1.fuente_ingreso_final_smmlv = 'ss' THEN (1- (t1.cuota_total_ce_smmlv + t1.cuota_total_ci_smmlv + t1.compras_basicas) / (t1.ingreso_final_smmlv))
        WHEN t1.fuente_ingreso_final_smmlv = 'fijo' THEN (1- (t1.cuota_total_ci_smmlv + t1.compras_basicas) / (t1.ingreso_final_smmlv))
        ELSE 0
    END AS regla_capacidad_pago
    ,CASE
        WHEN t1.valor_ingreso_estimadores_smmlv IS NULL OR t1.valor_ingreso_estimadores_smmlv = 0 THEN NULL
        WHEN t1.compras_basicas IS NULL
            THEN (1 - (t1.cuota_total_ce_smmlv) / (t1.valor_ingreso_estimadores_smmlv)) 
        WHEN t1.cuota_total_ce_smmlv IS NULL
            THEN (1 - (t1.compras_basicas) / (t1.valor_ingreso_estimadores_smmlv)) 
        ELSE
            (1 - (t1.compras_basicas + t1.cuota_total_ce_smmlv) / (t1.valor_ingreso_estimadores_smmlv)) 
    END AS capacidad_pago_ce_estimadores
    ,CASE
        WHEN t1.valor_ingreso_estimadores_smmlv IS NULL OR t1.valor_ingreso_estimadores_smmlv = 0 THEN NULL
        WHEN t1.compras_basicas IS NULL
            THEN (1 - (t1.cuota_total_ci_smmlv) / (t1.valor_ingreso_estimadores_smmlv)) 
        WHEN t1.cuota_total_ci_smmlv IS NULL
            THEN (1 - (t1.compras_basicas) / (t1.valor_ingreso_estimadores_smmlv)) 
        ELSE
            (1 - (t1.compras_basicas + t1.cuota_total_ci_smmlv) / (t1.valor_ingreso_estimadores_smmlv)) 
    END AS capacidad_pago_ci_estimadores
    ,CASE
        WHEN t1.valor_ingreso_estimadores_smmlv IS NULL OR t1.valor_ingreso_estimadores_smmlv = 0 THEN NULL
        WHEN t1.compras_basicas IS NULL
            THEN (1 - (t1.cuota_total_ci_smmlv + t1.cuota_total_ce_smmlv) / (t1.valor_ingreso_estimadores_smmlv)) 
        WHEN t1.cuota_total_ci_smmlv IS NULL
            THEN (1 - (t1.compras_basicas + t1.cuota_total_ce_smmlv) / (t1.valor_ingreso_estimadores_smmlv)) 
        WHEN t1.cuota_total_ce_smmlv IS NULL
            THEN (1 - (t1.compras_basicas + t1.cuota_total_ci_smmlv) / (t1.valor_ingreso_estimadores_smmlv))
        ELSE
            (1 - (t1.compras_basicas + t1.cuota_total_ce_smmlv + t1.cuota_total_ci_smmlv) / (t1.valor_ingreso_estimadores_smmlv))
    END AS capacidad_pago_total_estimadores

FROM
    tabla_cruce_categorias AS t1)

SELECT
    t1.year
    ,t1.month
    ,t1.cod_tipo_doc
    ,t1.cod_estado_cli
    ,t1.num_doc
    ,t1.cod_segm
    ,t1.dias_mora
    ,t1.desempleado
    ,t1.ingreso_final_smmlv
    ,t1.fuente_ingreso_final_smmlv
    ,t1.mnt_trx_var_smmlv
    ,t1.valor_ingreso_estimadores_smmlv
    ,t1.prom_cuenta_smmlv
    ,t1.max_cuenta_smmlv
    ,t1.min_cuenta_smmlv
    ,t1.vdbal_stddev_sld_dia_mes_cta_aho_smmlv
    ,t1.vdbal_coef_var_sld_mes_cta_aho_smmlv
    ,t1.compras_basicas
    ,t1.compras_no_basicas
    ,t1.razon_compras_basicas
    ,t1.razon_compras_no_basicas
    ,t1.flag_compras_basicas
    ,t1.flag_compras_no_basicas
    ,t1.flag_inversiones
    ,t1.cnt_total_inversiones
    ,t1.cuota_total_ce_smmlv
    ,t1.cuota_total_ci_smmlv
    ,t1.mnt_trx_cat_tdc
    ,t1.mnt_trx_cat_tdd 
    ,t1.mnt_trx_cat_retiros
    ,t1.gasto_total
    ,t1.razon_sobreendeudamiento_ce
    ,t1.razon_sobreendeudamiento_ci
    ,t1.razon_sobreendeudamiento_total
    ,t1.regla_razon_sobreendeudamiento
    ,t1.razon_sobreendeudamiento_ce_ingreso_estimadores
    ,t1.razon_sobreendeudamiento_ci_ingreso_estimadores
    ,t1.razon_sobreendeudamiento_total_ingreso_estimadores        
    ,CASE
        WHEN t1.razon_sobreendeudamiento_ce > 0.55 AND t1.cod_segm IN ('4','M') THEN 1
        WHEN t1.razon_sobreendeudamiento_ce > 0.60 AND t1.cod_segm IN ('6') THEN 1
        ELSE 0
    END AS flag_sobreendeudamiento_ce
    ,CASE
        WHEN t1.razon_sobreendeudamiento_ci > 0.55 AND t1.cod_segm IN ('4','M') THEN 1
        WHEN t1.razon_sobreendeudamiento_ci > 0.60 AND t1.cod_segm IN ('6') THEN 1
        ELSE 0
    END AS flag_sobreendeudamiento_ci
    ,CASE
        WHEN t1.razon_sobreendeudamiento_total > 0.55 AND t1.cod_segm IN ('4','M') THEN 1
        WHEN t1.razon_sobreendeudamiento_total > 0.60 AND t1.cod_segm IN ('6') THEN 1
        ELSE 0
    END AS flag_sobreendeudamiento_total
    ,CASE
        WHEN t1.regla_razon_sobreendeudamiento > 0.55 AND t1.cod_segm IN ('4','M') THEN 1
        WHEN t1.regla_razon_sobreendeudamiento > 0.60 AND t1.cod_segm IN ('6') THEN 1
        ELSE 0
    END AS flag_regla_sobreendeudamiento
    ,CASE
        WHEN t1.razon_sobreendeudamiento_ce_ingreso_estimadores > 0.55 AND t1.cod_segm IN ('4','M') THEN 1
        WHEN t1.razon_sobreendeudamiento_ce_ingreso_estimadores > 0.60 AND t1.cod_segm IN ('6') THEN 1
        ELSE 0
    END AS flag_regla_sobreendeudamiento_ce_ingreso_estimadores
    ,CASE
        WHEN t1.razon_sobreendeudamiento_ci_ingreso_estimadores > 0.55 AND t1.cod_segm IN ('4','M') THEN 1
        WHEN t1.razon_sobreendeudamiento_ci_ingreso_estimadores > 0.60 AND t1.cod_segm IN ('6') THEN 1
        ELSE 0
    END AS flag_regla_sobreendeudamiento_ci_ingreso_estimadores
    ,CASE
        WHEN t1.razon_sobreendeudamiento_total_ingreso_estimadores > 0.55 AND t1.cod_segm IN ('4','M') THEN 1
        WHEN t1.razon_sobreendeudamiento_total_ingreso_estimadores > 0.60 AND t1.cod_segm IN ('6') THEN 1
        ELSE 0
    END AS flag_regla_sobreendeudamiento_total_ingreso_estimadores
    ,t1.capacidad_pago_ce
    ,t1.capacidad_pago_ci
    ,t1.capacidad_pago_total
    ,t1.regla_capacidad_pago
    ,t1.capacidad_pago_ce_estimadores
    ,t1.capacidad_pago_ci_estimadores
    ,t1.capacidad_pago_total_estimadores
    ,CASE   
        WHEN t1.capacidad_pago_ce >= 0.35 THEN 1
        WHEN t1.capacidad_pago_ce < 0.35 THEN 0 
        ELSE NULL
    END AS flag_capacidad_pago_ce
    ,CASE   
        WHEN t1.capacidad_pago_ci >= 0.35 THEN 1
        WHEN t1.capacidad_pago_ci < 0.35 THEN 0 
        ELSE NULL
    END AS flag_capacidad_pago_ci
    ,CASE   
        WHEN t1.capacidad_pago_total >= 0.35 THEN 1
        WHEN t1.capacidad_pago_total < 0.35 THEN 0 
        ELSE NULL
    END AS flag_capacidad_pago_total
    ,CASE   
        WHEN t1.regla_capacidad_pago >= 0.35 THEN 1
        WHEN t1.regla_capacidad_pago < 0.35 THEN 0 
        ELSE NULL
    END AS flag_regla_capacidad_pago
    ,CASE   
        WHEN t1.capacidad_pago_ce_estimadores >= 0.35 THEN 1
        WHEN t1.capacidad_pago_ce_estimadores < 0.35 THEN 0 
        ELSE NULL
    END AS flag_capacidad_pago_ce_estimadores
    ,CASE   
        WHEN t1.capacidad_pago_ci_estimadores >= 0.35 THEN 1
        WHEN t1.capacidad_pago_ci_estimadores < 0.35 THEN 0 
        ELSE NULL
    END AS flag_capacidad_pago_ci_estimadores
    ,CASE   
        WHEN t1.capacidad_pago_total_estimadores >= 0.35 THEN 1
        WHEN t1.capacidad_pago_total_estimadores < 0.35 THEN 0 
        ELSE NULL
    END AS flag_capacidad_pago_total_estimadores
FROM
    b_sobreen AS t1;

COMPUTE STATS tabla_sobreendeudamiento;