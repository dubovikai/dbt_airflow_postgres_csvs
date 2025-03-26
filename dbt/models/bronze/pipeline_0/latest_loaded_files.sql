WITH latest_files AS (
    SELECT
        dataset,
        file_name,
        MAX(id) AS latest_id
    FROM {{ ref('base_file_load_log') }}
    GROUP BY dataset, file_name
)
SELECT file_log.*
FROM {{ ref('base_file_load_log') }} AS file_log
JOIN latest_files
    ON latest_files.latest_id = file_log.id
