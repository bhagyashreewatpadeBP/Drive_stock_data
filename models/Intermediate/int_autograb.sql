{{ config(
    materialized = "table"
) }}

WITH base AS (
    SELECT
        -- simple surrogate key using Snowflake SHA2 (no dbt_utils)
        SHA2(CONCAT_WS('|', vinnumber, stockno), 256) AS vehicle_sk,

        -- Cleaned string dimensions
        UPPER(make)            AS make,
        UPPER(model)           AS model,
        UPPER(series)          AS series,
        UPPER(variant)         AS variant,
        UPPER(bodytype)        AS body_type,
        UPPER(badge)           AS badge,
        UPPER(gearbox)         AS gearbox,
        UPPER(fueltype)        AS fuel_type,
        UPPER(drivetype)       AS drive_type,
        UPPER(trimcolour)      AS trim_colour,
        UPPER(bodycolour)      AS body_colour,

        -- Dealer details
        dealercode,
        dealername,
        dealeremail,
        dealerphone,
        dealerstate,
        dealersuburb,
        dealeraddress,
        dealerpostcode,
        dealerlicenseno,

        -- Numeric conversions
        TRY_TO_NUMBER(odometer)        AS odometer_km,
        TRY_TO_NUMBER(cylinders)       AS cylinders,
        TRY_TO_NUMBER(enginecapacity)  AS engine_cc,
        TRY_TO_NUMBER(imagecount)      AS image_count,
        TRY_TO_NUMBER(power)           AS power_kw,
        TRY_TO_NUMBER(seats)           AS seats_count,
        TRY_TO_NUMBER(doors)           AS door_count,

        -- Year and month
        TRY_TO_NUMBER(manuyear)   AS manufacture_year,
        TRY_TO_NUMBER(manumonth)  AS manufacture_month,
        TRY_TO_NUMBER(modelyear)  AS model_year,

        -- Stock & VIN
        stocktype,
        stockno,
        vinnumber,

        -- Derived easy fields
        CONCAT(make, ' ', model) AS make_model,

        CASE 
            WHEN TRY_TO_NUMBER(manuyear) IS NOT NULL 
            THEN YEAR(CURRENT_DATE()) - TRY_TO_NUMBER(manuyear)
        END AS vehicle_age,

        -- Additional attributes
        comments,
        options,
        photourllist,
        source_file_url
    FROM {{ ref('stg_autograb') }}
)

SELECT * FROM base
