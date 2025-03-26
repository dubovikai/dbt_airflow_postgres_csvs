WITH source AS (
    SELECT * FROM {{ source('bronze_layer', 'raw_voluum') }}
),

cleaned AS (
    SELECT
        CAST(COALESCE(source.date, REPLACE(latest_files.file_name, '.csv', '')) AS DATE) AS date,
        source.voluum_brand,
        CAST(source.clicks AS INTEGER) AS clicks,
        source.file_id,
        latest_files.loaded_at
    FROM source
    JOIN {{ ref('latest_loaded_files') }} AS latest_files
        ON source.file_id = latest_files.id
)

SELECT * FROM cleaned
