WITH source AS (
    SELECT * FROM {{ source('bronze_layer', 'raw_routy') }}
),

cleaned AS (
    SELECT
        CAST(COALESCE(source.date, REPLACE(latest_files.file_name, '.csv', '')) AS DATE) AS date,
        source.marketing_source,
        source.operator,
        source.country,
        -- source.countrycode,
        CAST(source.raw_earnings AS INTEGER) AS raw_earnings,
        CAST(source.visits AS INTEGER) AS visits,
        CAST(source.signups AS INTEGER) AS signups,
        latest_files.loaded_at
    FROM source
    JOIN {{ ref('latest_loaded_files') }} AS latest_files
        ON source.file_id = latest_files.id
)

SELECT * FROM cleaned
