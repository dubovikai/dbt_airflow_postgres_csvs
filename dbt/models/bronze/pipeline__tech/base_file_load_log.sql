WITH source AS (
    SELECT * FROM {{ source('bronze_layer', 'file_load_log') }}
),

cleaned AS (
    SELECT
        id,
        dataset,
        file_name,
        loaded_at
    FROM source
)

SELECT * FROM cleaned
