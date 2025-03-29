WITH source AS (
    SELECT * FROM {{ source('bronze_layer', 'raw_central_mapping') }}
),

cleaned AS (
    SELECT
        source.variants,
        source.std_operator,
        source.country,
        source.file_id,
        latest_files.loaded_at
    FROM source
    JOIN {{ ref('latest_loaded_files') }} AS latest_files
        ON source.file_id = latest_files.id
)

SELECT * FROM cleaned
