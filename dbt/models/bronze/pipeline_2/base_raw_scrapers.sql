WITH source AS (
    SELECT * FROM {{ source('bronze_layer', 'raw_scrapers') }}
),

cleaned AS (
    SELECT
        CAST(COALESCE(source.date, REPLACE(latest_files.file_name, '.csv', '')) AS DATE) AS date,
        marketing_source,
        operator,
        country,
        CAST(source.total_earnings AS INTEGER) AS total_earnings,
        CAST(source.visits AS INTEGER) AS visits,
        file_id
    FROM source
    JOIN {{ ref('latest_loaded_files') }} AS latest_files
        ON source.file_id = latest_files.id
)

SELECT * FROM cleaned
    