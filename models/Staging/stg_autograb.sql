{{ config(
    materialized = "table"
) }}

WITH source AS (
    SELECT
        TRIM(DAP)                AS dap,
        TRIM(EGC)                AS egc,
        TRIM(AGVI)               AS agvi,
        TRIM(MAKE)               AS make,
        TRIM(REGO)               AS rego,
        TRIM(BADGE)              AS badge,
        TRIM(DOORS)              AS doors,
        TRIM(GEARS)              AS gears,
        TRIM(MODEL)              AS model,
        TRIM(POWER)              AS power,
        TRIM(SEATS)              AS seats,
        TRIM(SERIES)             AS series,
        TRIM(GEARBOX)            AS gearbox,
        TRIM(OPTIONS)            AS options,
        TRIM(STOCKNO)            AS stockno,
        TRIM(VARIANT)            AS variant,
        TRIM(BODYTYPE)           AS bodytype,
        TRIM(COMMENTS)           AS comments,
        TRIM(FUELTYPE)           AS fueltype,
        TRIM(MANUYEAR)           AS manuyear,
        TRIM(ODOMETER)           AS odometer,
        TRIM(CYLINDERS)          AS cylinders,
        TRIM(DRIVETYPE)          AS drivetype,
        TRIM(MANUMONTH)          AS manumonth,
        TRIM(MODELYEAR)          AS modelyear,
        TRIM(STOCKTYPE)          AS stocktype,
        TRIM(VINNUMBER)          AS vinnumber,
        TRIM(BODYCOLOUR)         AS bodycolour,
        TRIM(DEALERCODE)         AS dealercode,
        TRIM(DEALERNAME)         AS dealername,
        TRIM(IMAGECOUNT)         AS imagecount,
        TRIM(TRIMCOLOUR)         AS trimcolour,
        TRIM(DEALEREMAIL)        AS dealeremail,
        TRIM(DEALERPHONE)        AS dealerphone,
        TRIM(DEALERSTATE)        AS dealerstate,
        TRIM(DEALERSUBURB)       AS dealersuburb,
        TRIM(PHOTOURLLIST)       AS photourllist,
        TRIM(DEALERADDRESS)      AS dealeraddress,
        TRIM(DEALERPOSTCODE)     AS dealerpostcode,
        TRIM(ENGINECAPACITY)     AS enginecapacity,
        TRIM(DEALERLICENSENO)    AS dealerlicenseno,
        TRIM(_AB_SOURCE_FILE_URL) AS source_file_url
    FROM {{ source('landing', 'AUTOGRAB') }}
)

SELECT * FROM source
