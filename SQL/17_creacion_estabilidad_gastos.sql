DROP TABLE IF EXISTS tabla_estabilidad_gastos PURGE;

CREATE TABLE tabla_estabilidad_gastos STORED AS PARQUET AS

-- Estabilidad o inestabilidad en los gastos de los clientes --
WITH promedio_gastos AS (
    -- Agregamos una columna para ordenar correctamente por fecha
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
        ,AVG(t1.compras_basicas) OVER (
            PARTITION BY t1.num_doc
            ORDER BY t1.fecha_orden
            ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
        ) AS media_compras_basicas
        ,AVG(t1.compras_no_basicas) OVER (
            PARTITION BY t1.num_doc
            ORDER BY t1.fecha_orden
            ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
        ) AS media_compras_no_basicas
        ,AVG(t1.compras_basicas + t1.compras_no_basicas) OVER (
            PARTITION BY t1.num_doc
            ORDER BY t1.fecha_orden
            ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
        ) AS media_suma_basicas_no_basicas
        ,AVG(t1.gasto_total) OVER (
            PARTITION BY t1.num_doc
            ORDER BY t1.fecha_orden
            ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
        ) AS media_gasto_total
    FROM tabla_ahorra AS t1
),

suma_cuadrados AS (
    -- Calculamos la suma de los cuadrados de las diferencias respecto a la media
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
        ,t1.media_compras_basicas
        ,t1.media_compras_no_basicas
        ,t1. media_suma_basicas_no_basicas
        ,t1.media_gasto_total
        ,SUM(POWER(t1.compras_basicas - t1.media_compras_basicas, 2)) OVER (
            PARTITION BY t1.num_doc
            ORDER BY t1.fecha_orden
            ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
        ) AS suma_cuadrados_6m_compras_basicas
        ,SUM(POWER(t1.compras_no_basicas - t1.media_compras_no_basicas, 2)) OVER (
            PARTITION BY t1.num_doc
            ORDER BY t1.fecha_orden
            ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
        ) AS suma_cuadrados_6m_compras_no_basicas
        ,SUM(POWER((t1.compras_basicas + t1.compras_no_basicas) - t1.media_suma_basicas_no_basicas, 2)) OVER (
            PARTITION BY t1.num_doc
            ORDER BY t1.fecha_orden
            ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
        ) AS suma_cuadrados_6m_suma_basicas_no_basicas
        ,SUM(POWER(t1.gasto_total - t1.media_gasto_total, 2)) OVER (
            PARTITION BY t1.num_doc
            ORDER BY t1.fecha_orden
            ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
        ) AS suma_cuadrados_6m_gasto_total
        ,COUNT(*) OVER (
            PARTITION BY t1.num_doc
            ORDER BY t1.fecha_orden
            ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
        ) AS count_6m
    FROM promedio_gastos AS t1
),

estabilidad AS (
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
        ,t1.media_compras_basicas
        ,t1.media_compras_no_basicas
        ,t1.media_suma_basicas_no_basicas
        ,t1.media_gasto_total
        ,t1.suma_cuadrados_6m_compras_basicas
        ,t1.suma_cuadrados_6m_compras_no_basicas
        ,t1.suma_cuadrados_6m_suma_basicas_no_basicas
        ,t1.suma_cuadrados_6m_gasto_total
        ,t1.count_6m
        ,SQRT(t1.suma_cuadrados_6m_compras_basicas / (t1.count_6m - 1)) AS sigma_compras_basicas
        ,SQRT(t1.suma_cuadrados_6m_compras_no_basicas / (t1.count_6m - 1)) AS sigma_compras_no_basicas
        ,SQRT(t1.suma_cuadrados_6m_suma_basicas_no_basicas / (t1.count_6m - 1)) AS sigma_suma_basicas_no_basicas
        ,SQRT(t1.suma_cuadrados_6m_gasto_total / (t1.count_6m - 1)) AS sigma_gasto_total

        -- Clasificamos la estabilidad del ingreso_final_smmlv
        ,CASE
            WHEN t1.count_6m < 6 THEN NULL -- Si no hay suficientes datos, se evita clasificación incorrecta
            WHEN t1.compras_basicas BETWEEN t1.media_compras_basicas - 2 * SQRT(t1.suma_cuadrados_6m_compras_basicas / (t1.count_6m - 1)) AND t1.media_compras_basicas + 2 * SQRT(t1.suma_cuadrados_6m_compras_basicas / (t1.count_6m - 1)) THEN 1
            ELSE 0
        END AS flag_estabilidad_mensual_compras_basicas
        ,CASE
            WHEN t1.count_6m < 6 THEN NULL -- Si no hay suficientes datos, se evita clasificación incorrecta
            WHEN t1.compras_no_basicas BETWEEN t1.media_compras_no_basicas - 2 * SQRT(t1.suma_cuadrados_6m_compras_no_basicas / (t1.count_6m - 1)) AND t1.media_compras_no_basicas + 2 * SQRT(t1.suma_cuadrados_6m_compras_no_basicas / (t1.count_6m - 1)) THEN 1
            ELSE 0
        END AS flag_estabilidad_mensual_compras_no_basicas
        ,CASE
            WHEN t1.count_6m < 6 THEN NULL -- Si no hay suficientes datos, se evita clasificación incorrecta
            WHEN (t1.compras_basicas + t1.compras_no_basicas) BETWEEN t1.media_suma_basicas_no_basicas - 2 * SQRT(t1.suma_cuadrados_6m_suma_basicas_no_basicas / (t1.count_6m - 1)) AND t1.media_suma_basicas_no_basicas + 2 * SQRT(t1.suma_cuadrados_6m_suma_basicas_no_basicas / (t1.count_6m - 1)) THEN 1
            ELSE 0
        END AS flag_estabilidad_mensual_suma_basicas_no_basicas
        ,CASE
            WHEN t1.count_6m < 6 THEN NULL -- Si no hay suficientes datos, se evita clasificación incorrecta
            WHEN t1.gasto_total BETWEEN t1.media_gasto_total - 2 * SQRT(t1.suma_cuadrados_6m_gasto_total / (t1.count_6m - 1)) AND t1.media_gasto_total + 2 * SQRT(t1.suma_cuadrados_6m_gasto_total / (t1.count_6m - 1)) THEN 1
            ELSE 0
        END AS flag_estabilidad_mensual_gasto_total

    FROM suma_cuadrados AS t1
ORDER BY t1.num_doc, t1.year, t1.month
),


flag_estabilidad AS (
    -- Calculamos la suma de los cuadrados de las diferencias respecto a la media
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
        ,t1.media_compras_basicas
        ,t1.media_compras_no_basicas
        ,t1. media_suma_basicas_no_basicas
        ,t1.media_gasto_total
        ,t1.suma_cuadrados_6m_compras_basicas
        ,t1.suma_cuadrados_6m_compras_no_basicas
        ,t1.suma_cuadrados_6m_suma_basicas_no_basicas
        ,t1.suma_cuadrados_6m_gasto_total
        ,t1.count_6m
        ,t1.sigma_compras_basicas
        ,t1.sigma_compras_no_basicas
        ,t1.sigma_suma_basicas_no_basicas
        ,t1.sigma_gasto_total
        ,t1.flag_estabilidad_mensual_compras_basicas
        ,t1.flag_estabilidad_mensual_compras_no_basicas
        ,t1.flag_estabilidad_mensual_suma_basicas_no_basicas
        ,t1.flag_estabilidad_mensual_gasto_total

        -- Contamos solo los casos donde la estabilidad en la ventana de 6 meses es 'Inestable'
        ,COUNT(CASE WHEN t1.flag_estabilidad_mensual_compras_basicas = 0 THEN 1 END) OVER (
            PARTITION BY t1.num_doc
            ORDER BY t1.fecha_orden
            ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
        ) AS count_inestables_6m_compras_basicas

        ,COUNT(CASE WHEN t1.flag_estabilidad_mensual_compras_no_basicas = 0 THEN 1 END) OVER (
            PARTITION BY t1.num_doc
            ORDER BY t1.fecha_orden
            ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
        ) AS count_inestables_6m_compras_no_basicas

        ,COUNT(CASE WHEN t1.flag_estabilidad_mensual_suma_basicas_no_basicas = 0 THEN 1 END) OVER (
            PARTITION BY t1.num_doc
            ORDER BY t1.fecha_orden
            ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
        ) AS count_inestables_6m_suma_basicas_no_basicas

        ,COUNT(CASE WHEN t1.flag_estabilidad_mensual_gasto_total = 0 THEN 1 END) OVER (
            PARTITION BY t1.num_doc
            ORDER BY t1.fecha_orden
            ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
        ) AS count_inestables_6m_gasto_total
        ,CASE
            WHEN t1.ingreso_final_smmlv = 0 THEN NULL
            ELSE t1.mnt_trx_cat_retiros / t1.ingreso_final_smmlv
        END AS razon_retiro_efectivo_ingreso_final
        ,CASE
            WHEN t1.valor_ingreso_estimadores_smmlv = 0 THEN NULL
            ELSE t1.mnt_trx_cat_retiros / t1.valor_ingreso_estimadores_smmlv
        END AS razon_retiro_efectivo_estimadores

    FROM estabilidad AS t1
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
    ,IF(t1.count_inestables_6m_compras_basicas > 2, 0, 1) AS flag_estabilidad_6m_compras_basicas
    ,IF(t1.count_inestables_6m_compras_no_basicas > 2, 0, 1) AS flag_estabilidad_6m_compras_no_basicas
    ,IF(t1.count_inestables_6m_suma_basicas_no_basicas > 2, 0, 1) AS flag_estabilidad_6m_suma_basicas_no_basicas
    ,IF(t1.count_inestables_6m_gasto_total > 2, 0, 1) AS flag_estabilidad_6m_gasto_total
    ,IF(t1.razon_retiro_efectivo_ingreso_final < 0.33, 1, 0 ) AS flag_retiro_efectivo_bajo_ingreso_final
    ,IF(t1.razon_retiro_efectivo_estimadores < 0.33, 1, 0 ) AS flag_retiro_efectivo_bajo_estimadores

FROM
    flag_estabilidad AS t1;

COMPUTE STATS tabla_estabilidad_gastos;