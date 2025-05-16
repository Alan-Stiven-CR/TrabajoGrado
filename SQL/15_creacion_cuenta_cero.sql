DROP TABLE IF EXISTS tabla_cuenta_cero PURGE;

CREATE TABLE tabla_cuenta_cero STORED AS PARQUET AS

WITH conteo_cuenta_cero AS (
SELECT
    t1.fecha_orden 
    ,t1.year
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
    ,t1.flag_sobreendeudamiento_ce
    ,t1.flag_sobreendeudamiento_ci
    ,t1.flag_sobreendeudamiento_total
    ,t1.flag_regla_sobreendeudamiento
    ,t1.flag_regla_sobreendeudamiento_ce_ingreso_estimadores
    ,t1.flag_regla_sobreendeudamiento_ci_ingreso_estimadores
    ,t1.flag_regla_sobreendeudamiento_total_ingreso_estimadores
    ,t1.capacidad_pago_ce
    ,t1.capacidad_pago_ci
    ,t1.capacidad_pago_total
    ,t1.regla_capacidad_pago
    ,t1.capacidad_pago_ce_estimadores
    ,t1.capacidad_pago_ci_estimadores
    ,t1.capacidad_pago_total_estimadores
    ,t1.flag_capacidad_pago_ce
    ,t1.flag_capacidad_pago_ci
    ,t1.flag_capacidad_pago_total
    ,t1.flag_regla_capacidad_pago
    ,t1.flag_capacidad_pago_ce_estimadores
    ,t1.flag_capacidad_pago_ci_estimadores
    ,t1.flag_capacidad_pago_total_estimadores   
    ,t1.estabilidad_mensual
    ,t1.estabilidad_mensual_estimadores
    ,t1.flag_estabilidad_ingreso
    ,t1.flag_estabilidad_ingreso_estimadores
    ,SUM(CASE 
            WHEN t1.min_cuenta_smmlv IS NULL THEN NULL
            WHEN t1.min_cuenta_smmlv <= 0 THEN 1
            ELSE 0
        END) OVER (
            PARTITION BY t1.num_doc 
            ORDER BY t1.fecha_orden 
            ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
    ) AS count_cuenta_cero_6m
FROM
    tabla_estabilidad_ingresos AS t1
)

SELECT
    t1.fecha_orden 
    ,t1.year
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
    ,t1.flag_sobreendeudamiento_ce
    ,t1.flag_sobreendeudamiento_ci
    ,t1.flag_sobreendeudamiento_total
    ,t1.flag_regla_sobreendeudamiento
    ,t1.flag_regla_sobreendeudamiento_ce_ingreso_estimadores
    ,t1.flag_regla_sobreendeudamiento_ci_ingreso_estimadores
    ,t1.flag_regla_sobreendeudamiento_total_ingreso_estimadores
    ,t1.capacidad_pago_ce
    ,t1.capacidad_pago_ci
    ,t1.capacidad_pago_total
    ,t1.regla_capacidad_pago
    ,t1.capacidad_pago_ce_estimadores
    ,t1.capacidad_pago_ci_estimadores
    ,t1.capacidad_pago_total_estimadores
    ,t1.flag_capacidad_pago_ce
    ,t1.flag_capacidad_pago_ci
    ,t1.flag_capacidad_pago_total
    ,t1.flag_regla_capacidad_pago
    ,t1.flag_capacidad_pago_ce_estimadores
    ,t1.flag_capacidad_pago_ci_estimadores
    ,t1.flag_capacidad_pago_total_estimadores   
    ,t1.estabilidad_mensual
    ,t1.estabilidad_mensual_estimadores
    ,t1.flag_estabilidad_ingreso
    ,t1.flag_estabilidad_ingreso_estimadores
    ,IF(t1.count_cuenta_cero_6m >= 6, 1, 0) AS flag_cuenta_cero
FROM
    conteo_cuenta_cero AS t1;

COMPUTE STATS tabla_cuenta_cero;