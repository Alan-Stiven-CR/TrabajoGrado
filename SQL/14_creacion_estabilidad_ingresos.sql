DROP TABLE IF EXISTS tabla_estabilidad_ingresos PURGE;

CREATE TABLE tabla_estabilidad_ingresos STORED AS PARQUET AS

-- Estabilidad o inestabilidad en los ingresos de los clientes --
WITH ingresos_ordenados AS (
    -- Agregamos una columna para ordenar correctamente por fecha
    SELECT 
        t1.year*100 + t1.month AS fecha_orden -- Convertimos anio + month en un número (AAAAMM)
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
    FROM tabla_sobreendeudamiento AS t1
),
media_movil AS (
    -- Calculamos la media móvil sobre los últimos 6 ingestion_monthes
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
        ,AVG(t1.ingreso_final_smmlv) OVER (
            PARTITION BY t1.num_doc 
            ORDER BY t1.fecha_orden 
            ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
        ) AS media_6m
        ,AVG(t1.valor_ingreso_estimadores_smmlv) OVER (
            PARTITION BY t1.num_doc 
            ORDER BY t1.fecha_orden 
            ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
        ) AS media_6m_estimadores

    FROM ingresos_ordenados AS t1
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
        ,t1.media_6m
        ,t1.media_6m_estimadores
        ,SUM(POWER(t1.ingreso_final_smmlv - t1.media_6m, 2)) OVER (
            PARTITION BY t1.num_doc 
            ORDER BY t1.fecha_orden 
            ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
        ) AS suma_cuadrados_6m
        ,SUM(POWER(t1.valor_ingreso_estimadores_smmlv - t1.media_6m_estimadores, 2)) OVER (
            PARTITION BY t1.num_doc 
            ORDER BY t1.fecha_orden 
            ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
        ) AS suma_cuadrados_6m_estimadores
        ,COUNT(*) OVER (
            PARTITION BY t1.num_doc 
            ORDER BY t1.fecha_orden 
            ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
        ) AS count_6m
        
    FROM media_movil AS t1
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
    ,t1.media_6m
    ,t1.media_6m_estimadores
    ,t1.suma_cuadrados_6m
    ,t1.suma_cuadrados_6m_estimadores
    ,t1.count_6m
    ,SQRT(t1.suma_cuadrados_6m / (t1.count_6m - 1)) AS sigma_6m
    ,SQRT(t1.suma_cuadrados_6m_estimadores / (t1.count_6m - 1)) AS sigma_6m_estimadores
    -- Clasificamos la estabilidad del ingreso_final_smmlv
    ,CASE 
        WHEN t1.count_6m < 6 THEN NULL -- Si no hay suficientes datos, se evita clasificación incorrecta
        WHEN t1.ingreso_final_smmlv BETWEEN t1.media_6m - 2 * SQRT(t1.suma_cuadrados_6m / (t1.count_6m - 1)) AND t1.media_6m + 2 * SQRT(t1.suma_cuadrados_6m / (t1.count_6m - 1)) THEN 1
        ELSE 0
    END AS estabilidad_mensual
    ,CASE 
        WHEN t1.count_6m < 6 THEN NULL -- Si no hay suficientes datos, se evita clasificación incorrecta
        WHEN t1.valor_ingreso_estimadores_smmlv BETWEEN t1.media_6m_estimadores - 2 * SQRT(t1.suma_cuadrados_6m_estimadores / (t1.count_6m - 1)) AND t1.media_6m_estimadores + 2 * SQRT(t1.suma_cuadrados_6m_estimadores / (t1.count_6m - 1)) THEN 1
        ELSE 0
    END AS estabilidad_mensual_estimadores
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
        ,t1.media_6m
        ,t1.media_6m_estimadores
        ,t1.suma_cuadrados_6m
        ,t1.suma_cuadrados_6m_estimadores
        ,t1.count_6m
        ,t1.sigma_6m
        ,t1.sigma_6m_estimadores
        ,t1.estabilidad_mensual
        ,t1.estabilidad_mensual_estimadores
        -- Contamos solo los casos donde la estabilidad en la ventana de 6 meses es 'Inestable'
        ,COUNT(CASE WHEN t1.estabilidad_mensual = 0 THEN 1 END) OVER (
            PARTITION BY t1.num_doc 
            ORDER BY t1.fecha_orden 
            ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
        ) AS count_inestables_6m
        ,COUNT(CASE WHEN t1.estabilidad_mensual_estimadores = 0 THEN 1 END) OVER (
            PARTITION BY t1.num_doc 
            ORDER BY t1.fecha_orden 
            ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
        ) AS count_inestables_6m_estimadores
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
    ,IF(t1.count_inestables_6m > 2, 0, 1) AS flag_estabilidad_ingreso
    ,IF(t1.count_inestables_6m_estimadores > 2, 0, 1) AS flag_estabilidad_ingreso_estimadores
FROM
    flag_estabilidad AS t1;

COMPUTE STATS tabla_estabilidad_ingresos;