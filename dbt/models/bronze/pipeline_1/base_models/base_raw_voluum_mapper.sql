WITH source AS (
    SELECT * FROM {{ source('bronze_layer', 'raw_voluum_mapper') }}
),

cleaned AS (
    SELECT
        source.voluum_brand,
        source.marketing_source,
        source.file_id,
        latest_files.loaded_at
    FROM source
    JOIN {{ ref('latest_loaded_files') }} AS latest_files
        ON source.file_id = latest_files.id
)

SELECT * FROM cleaned
