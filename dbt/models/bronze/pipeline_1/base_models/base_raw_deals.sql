WITH source AS (
    SELECT * FROM {{ source('bronze_layer', 'raw_deals') }}
),

cleaned AS (
    SELECT
        source.marketing_source,
        source.deal,
        REPLACE(source.comments, 'total commision=', '') AS calc_formula,
        source.file_id,
        latest_files.loaded_at
    FROM source
    JOIN {{ ref('latest_loaded_files') }} AS latest_files
        ON source.file_id = latest_files.id
)
SELECT * FROM cleaned
