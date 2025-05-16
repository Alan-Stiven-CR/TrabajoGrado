DROP TABLE IF EXISTS tabla_ingresos_personales PURGE;

CREATE TABLE tabla_ingresos_personales STORED AS PARQUET AS

WITH ingresos AS (
    SELECT
        num_doc
        ,cod_tipo_doc
        ,f_corte
        ,f_analisis
        ,f_consulta_ss
        ,ingreso_ss
        ,fuente_ss
        ,ingresos_fopep
        ,valor_ingreso_total_pic
        ,ing_fijos_empleado_pic
        ,ing_variables_empleado_pic
        ,otros_ingresos_pic
        ,year
        ,month
        ,cnt_trx_fijos
        ,mnt_trx_fijos
        ,mnt_trx_var
    FROM
        ingresos_personales
    WHERE
        year IN (2025, 2024, 2023, 2022, 2021)
)

SELECT 
    t1.num_doc
    ,t1.cod_tipo_doc
    ,t1.f_corte
    ,t1.f_analisis
    ,t1.f_consulta_ss
    ,(t1.ingreso_ss / t2.salario) AS ingreso_ss_smmlv
    ,t1.fuente_ss
    ,t1.ingresos_fopep
    ,(t1.valor_ingreso_total_pic / t2.salario) AS valor_ingreso_total_pic_smmlv
    ,t1.ing_fijos_empleado_pic
    ,t1.ing_variables_empleado_pic
    ,t1.otros_ingresos_pic
    ,t1.year
    ,t1.month
    ,t1.cnt_trx_fijos
    ,(t1.mnt_trx_fijos / t2.salario) AS mnt_trx_fijos_smmlv
    ,(t1.mnt_trx_var / t2.salario) AS mnt_trx_var_smmlv
FROM 
    ingresos AS t1
LEFT JOIN
    salario_minimo_colombia AS t2
ON
    t1.year = t2.year;

COMPUTE STATS tabla_ingresos_personales;
