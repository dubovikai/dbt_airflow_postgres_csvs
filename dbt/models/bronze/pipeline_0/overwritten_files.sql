SELECT
    id,
    dataset,
    file_name,
    loaded_at,
    LEAD(loaded_at) OVER (PARTITION BY dataset, file_name ORDER BY loaded_at) AS overwritten_at
FROM {{ ref('base_file_load_log') }} AS file_log
ORDER BY dataset, file_name, loaded_at DESC


