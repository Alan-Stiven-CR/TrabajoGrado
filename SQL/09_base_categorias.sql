DROP TABLE IF EXISTS tabla_modulos_categorias PURGE;

CREATE TABLE tabla_modulos_categorias STORED AS PARQUET AS

SELECT
    f_corte
    ,year
    ,month
    ,num_doc
    ,cod_tipo_doc
    ,MAX(mnt_trx_cat_tdc) AS mnt_trx_cat_tdc -- Monto en salarios mínimos de las transacciones por el producto Tarjeta de Crédito
    ,MAX(mnt_trx_cat_tdd) AS mnt_trx_cat_tdd -- Monto en salarios mínimos de las transacciones por el producto Tarjeta débito
    ,MAX(mnt_trx_cat_retiros) AS mnt_trx_cat_retiros -- Monto en salarios mínimos de las transacciones por Retiros en efectivo
    ,MAX(mnt_trx_cat_credits) AS mnt_trx_cat_credits -- cuotas financieras cat
    ,CASE 
        WHEN 
            MAX(mnt_trx_cat_bills_utilities) IS NULL AND
            MAX(mnt_trx_cat_credits) IS NULL AND
            MAX(mnt_trx_cat_food) IS NULL AND
            MAX(mnt_trx_cat_home_life) IS NULL AND
            MAX(mnt_trx_cat_education) IS NULL AND
            MAX(mnt_trx_cat_pets) IS NULL AND
            MAX(mnt_trx_cat_insurance) IS NULL AND
            MAX(mnt_trx_cat_invest) IS NULL AND
            MAX(mnt_trx_cat_online) IS NULL AND
            MAX(mnt_trx_cat_leisure) IS NULL AND
            MAX(mnt_trx_cat_personal_shopping) IS NULL AND
            MAX(mnt_trx_cat_taxes) IS NULL AND
            MAX(mnt_trx_cat_transport) IS NULL AND
            MAX(mnt_trx_cat_travel) IS NULL AND
            MAX(mnt_trx_cat_others) IS NULL AND
            MAX(mnt_trx_cat_transfers) IS NULL AND
            MAX(mnt_trx_cat_withdrawal) IS NULL AND
            MAX(mnt_trx_cat_app) IS NULL AND
            MAX(mnt_trx_cat_boton) IS NULL AND
            MAX(mnt_trx_cat_life_coach_transf) IS NULL AND
            MAX(mnt_trx_cat_pse) IS NULL AND
            MAX(mnt_trx_cat_qr) IS NULL AND
            MAX(mnt_trx_cat_recaudo) IS NULL AND
            MAX(mnt_trx_cat_retiros) IS NULL AND
            MAX(mnt_trx_cat_tdc) IS NULL AND
            MAX(mnt_trx_cat_tdd) IS NULL
        THEN NULL
        ELSE
            COALESCE(MAX(mnt_trx_cat_bills_utilities), 0) +
            COALESCE(MAX(mnt_trx_cat_credits), 0) +
            COALESCE(MAX(mnt_trx_cat_food), 0) +
            COALESCE(MAX(mnt_trx_cat_home_life), 0) +
            COALESCE(MAX(mnt_trx_cat_education), 0) +
            COALESCE(MAX(mnt_trx_cat_pets), 0) +
            COALESCE(MAX(mnt_trx_cat_insurance), 0) +
            COALESCE(MAX(mnt_trx_cat_invest), 0) +
            COALESCE(MAX(mnt_trx_cat_online), 0) +
            COALESCE(MAX(mnt_trx_cat_leisure), 0) +
            COALESCE(MAX(mnt_trx_cat_personal_shopping), 0) +
            COALESCE(MAX(mnt_trx_cat_taxes), 0) +
            COALESCE(MAX(mnt_trx_cat_transport), 0) +
            COALESCE(MAX(mnt_trx_cat_travel), 0) +
            COALESCE(MAX(mnt_trx_cat_others), 0) +
            COALESCE(MAX(mnt_trx_cat_transfers), 0) +
            COALESCE(MAX(mnt_trx_cat_withdrawal), 0) +
            COALESCE(MAX(mnt_trx_cat_app), 0) +
            COALESCE(MAX(mnt_trx_cat_boton), 0) +
            COALESCE(MAX(mnt_trx_cat_life_coach_transf), 0) +
            COALESCE(MAX(mnt_trx_cat_pse), 0) +
            COALESCE(MAX(mnt_trx_cat_qr), 0) +
            COALESCE(MAX(mnt_trx_cat_recaudo), 0) +
            COALESCE(MAX(mnt_trx_cat_retiros), 0) +
            COALESCE(MAX(mnt_trx_cat_tdc), 0) +
            COALESCE(MAX(mnt_trx_cat_tdd), 0)
    END AS gasto_total

FROM 
    modulos_categorias
WHERE 
    year IN (2025, 2024, 2023, 2022, 2021)
GROUP BY
    f_corte
    ,year
    ,month
    ,num_doc
    ,cod_tipo_doc;

COMPUTE STATS tabla_modulos_categorias;
