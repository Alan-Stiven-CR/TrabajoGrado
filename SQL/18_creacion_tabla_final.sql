DROP TABLE IF EXISTS tabla_cupo_compras_avances PURGE;

CREATE TABLE tabla_cupo_compras_avances STORED AS PARQUET AS

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
    ,t1.flag_cuenta_cero
    ,t1.razon_ahorro_cliente
    ,t1.razon_ahorro_cliente_estimadores
    ,t1.flag_cliente_ahorra
    ,t1.flag_cliente_ahorra_estimadores
    ,t1.flag_estabilidad_mensual_compras_basicas
    ,t1.flag_estabilidad_mensual_compras_no_basicas
    ,t1.flag_estabilidad_mensual_suma_basicas_no_basicas
    ,t1.flag_estabilidad_mensual_gasto_total
    ,t1.razon_retiro_efectivo_ingreso_final
    ,t1.razon_retiro_efectivo_estimadores
    ,t1.flag_estabilidad_6m_compras_basicas
    ,t1.flag_estabilidad_6m_compras_no_basicas
    ,t1.flag_estabilidad_6m_suma_basicas_no_basicas
    ,t1.flag_estabilidad_6m_gasto_total
    ,t1.flag_retiro_efectivo_bajo_ingreso_final
    ,t1.flag_retiro_efectivo_bajo_estimadores
    ,t2.cupo_smmlv
    ,t2.val_trx_cop_smmlv
    ,t2.val_trx_cop_avance_smmlv
    ,IF((t2.val_trx_cop_avance_smmlv / t2.cupo_smmlv) > 0.4, 1, 0 ) AS flag_avances_cupo_uso_tdc
    ,t2.val_trx_cop_smmlv / t1.valor_ingreso_estimadores_smmlv AS razon_trx_max_una_cuota_ingreso_estimadores
    ,IF(t2.val_trx_cop_smmlv / t1.valor_ingreso_estimadores_smmlv >= 0.5, 1, 0) AS flag_mal_uso_tdc_trx_max_ingreso_estimadores
    ,t3.valor_g
FROM
    tabla_estabilidad_gastos AS t1
LEFT JOIN
    tabla_cupos AS t2
    ON 
        t1.num_doc = t2.num_doc
        AND t1.cod_tipo_doc = t2.cod_tipo_doc
        AND t1.year = t2.year
        AND t1.month = t2.month
LEFT JOIN
    tabla_grupos_riesgo AS t3
    ON 
        t1.num_doc = t2.num_doc
        AND t1.cod_tipo_doc = t2.cod_tipo_doc
        AND t1.year = t2.year
        AND t1.month = t2.month;

COMPUTE STATS tabla_cupo_compras_avances;