DROP TABLE IF EXISTS tabla_modulos_subcategorias PURGE;

CREATE TABLE tabla_modulos_subcategorias STORED AS PARQUET AS

SELECT 
    f_corte
    ,year
    ,month
    ,num_doc
    ,cod_tipo_doc
    -- Sumar valores de categorías básicas, ignorando NULLs, pero dejando NULL si todas son NULL
    ,CASE 
        WHEN 

            -- bills & utilities
            MAX(mnt_trx_subcat_public_services) IS NULL AND
            MAX(mnt_trx_subcat_rent) IS NULL AND
            MAX(mnt_trx_subcat_telecommunications) IS NULL AND

            -- -- education
            MAX(mnt_trx_subcat_education) IS NULL AND

            -- transport
            MAX(mnt_trx_subcat_gas) IS NULL AND
            MAX(mnt_trx_subcat_public_transit) IS NULL AND

            -- Home & life 
            MAX(mnt_trx_subcat_health) IS NULL AND

            -- insurance
            MAX(mnt_trx_subcat_insurance_health) IS NULL AND

            -- food
            MAX(mnt_trx_subcat_market_hd) IS NULL AND
            MAX(mnt_trx_subcat_market_premium) IS NULL AND

            -- pets
            MAX(mnt_trx_subcat_pets) IS NULL
        THEN NULL

        ELSE 
            COALESCE(MAX(mnt_trx_subcat_public_services), 0) +
            COALESCE(MAX(mnt_trx_subcat_rent), 0) +
            COALESCE(MAX(mnt_trx_subcat_telecommunications), 0) +
            COALESCE(MAX(mnt_trx_subcat_education), 0) +
            COALESCE(MAX(mnt_trx_subcat_gas), 0) +
            COALESCE(MAX(mnt_trx_subcat_public_transit), 0) +
            COALESCE(MAX(mnt_trx_subcat_health), 0) +
            COALESCE(MAX(mnt_trx_subcat_insurance_health), 0) +
            COALESCE(MAX(mnt_trx_subcat_market_hd), 0) +
            COALESCE(MAX(mnt_trx_subcat_market_premium), 0) +
            COALESCE(MAX(mnt_trx_subcat_pets), 0)
    END AS compras_basicas,

    -- Sumar valores de categorías no básicas, ignorando NULLs, pero dejando NULL si todas son NULL
    CASE 
        WHEN 

            -- travel
            MAX(mnt_trx_subcat_accommodation) IS NULL AND
            MAX(mnt_trx_subcat_auto_rental) IS NULL AND
            MAX(mnt_trx_subcat_flights) IS NULL AND
            MAX(mnt_trx_subcat_travel_others) IS NULL AND

            -- transport
            MAX(mnt_trx_subcat_auto_expenses) IS NULL AND
            MAX(mnt_trx_subcat_parking) IS NULL AND
            MAX(mnt_trx_subcat_taxis) IS NULL AND

            -- insurance
            MAX(mnt_trx_subcat_insurance_auto) IS NULL AND
            MAX(mnt_trx_subcat_insurance_life) IS NULL AND
            MAX(mnt_trx_subcat_insurance_others) IS NULL AND

            -- leisure
            MAX(mnt_trx_subcat_bars) IS NULL AND
            MAX(mnt_trx_subcat_betting) IS NULL AND
            MAX(mnt_trx_subcat_events) IS NULL AND
            MAX(mnt_trx_subcat_gaming) IS NULL AND
            MAX(mnt_trx_subcat_leisure_others) IS NULL AND
            MAX(mnt_trx_subcat_movies) IS NULL AND

            -- home & life
            MAX(mnt_trx_subcat_beauty) IS NULL AND
            MAX(mnt_trx_subcat_fitness) IS NULL AND
            MAX(mnt_trx_subcat_home) IS NULL AND
            MAX(mnt_trx_subcat_sports) IS NULL AND

            -- personal shopping
            MAX(mnt_trx_subcat_clothing) IS NULL AND
            MAX(mnt_trx_subcat_luxury) IS NULL AND
            MAX(mnt_trx_subcat_technology) IS NULL AND

            -- food
            MAX(mnt_trx_subcat_coffee) IS NULL AND
            MAX(mnt_trx_subcat_delivery) IS NULL AND
            MAX(mnt_trx_subcat_restaurants) IS NULL AND
            MAX(mnt_trx_subcat_snacks) IS NULL AND

            --  Investments & Savings 
            MAX(mnt_trx_subcat_crypto) IS NULL AND
            MAX(mnt_trx_subcat_stocks) IS NULL AND

            -- online paltforms
            MAX(mnt_trx_subcat_streaming) IS NULL AND

            -- taxes
            MAX(mnt_trx_subcat_taxes_auto) IS NULL AND 
            MAX(mnt_trx_subcat_taxes_government) IS NULL
        THEN NULL

        ELSE 
            COALESCE(MAX(mnt_trx_subcat_accommodation), 0) +
            COALESCE(MAX(mnt_trx_subcat_auto_rental), 0) +
            COALESCE(MAX(mnt_trx_subcat_flights), 0) +
            COALESCE(MAX(mnt_trx_subcat_travel_others), 0) +
            COALESCE(MAX(mnt_trx_subcat_auto_expenses), 0) +
            COALESCE(MAX(mnt_trx_subcat_parking), 0) +
            COALESCE(MAX(mnt_trx_subcat_taxis), 0) +
            COALESCE(MAX(mnt_trx_subcat_insurance_auto), 0) +
            COALESCE(MAX(mnt_trx_subcat_insurance_life), 0) +
            COALESCE(MAX(mnt_trx_subcat_insurance_others), 0) +
            COALESCE(MAX(mnt_trx_subcat_bars), 0) +
            COALESCE(MAX(mnt_trx_subcat_betting), 0) +
            COALESCE(MAX(mnt_trx_subcat_events), 0) +
            COALESCE(MAX(mnt_trx_subcat_gaming), 0) +
            COALESCE(MAX(mnt_trx_subcat_leisure_others), 0) +
            COALESCE(MAX(mnt_trx_subcat_movies), 0) +
            COALESCE(MAX(mnt_trx_subcat_beauty), 0) +
            COALESCE(MAX(mnt_trx_subcat_fitness), 0) +
            COALESCE(MAX(mnt_trx_subcat_home), 0) +
            COALESCE(MAX(mnt_trx_subcat_sports), 0) +
            COALESCE(MAX(mnt_trx_subcat_clothing), 0) +
            COALESCE(MAX(mnt_trx_subcat_luxury), 0) +
            COALESCE(MAX(mnt_trx_subcat_technology), 0) +
            COALESCE(MAX(mnt_trx_subcat_coffee), 0) +
            COALESCE(MAX(mnt_trx_subcat_delivery), 0) +
            COALESCE(MAX(mnt_trx_subcat_restaurants), 0) +
            COALESCE(MAX(mnt_trx_subcat_snacks), 0) +
            COALESCE(MAX(mnt_trx_subcat_crypto), 0) +
            COALESCE(MAX(mnt_trx_subcat_stocks), 0) +
            COALESCE(MAX(mnt_trx_subcat_streaming), 0) +
            COALESCE(MAX(mnt_trx_subcat_taxes_auto), 0) +
            COALESCE(MAX(mnt_trx_subcat_taxes_government), 0) +
            COALESCE(MAX(mnt_trx_subcat_pets), 0)
    END AS compras_no_basicas
FROM 
    modulos_subcategorias
WHERE 
    year IN (2025, 2024, 2023, 2022, 2021)
GROUP BY
    f_corte
    ,year
    ,month
    ,num_doc
    ,cod_tipo_doc;

COMPUTE STATS tabla_modulos_subcategorias;
