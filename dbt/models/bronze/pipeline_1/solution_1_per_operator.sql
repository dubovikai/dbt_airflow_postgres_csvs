WITH union_data AS (
    SELECT
        raw_routy.date,
        raw_routy.operator,
        raw_routy.raw_earnings
    FROM {{ ref('base_raw_routy') }} AS raw_routy
    UNION ALL
    SELECT
        raw_manual.date,
        raw_manual.operator,
        raw_manual.raw_earnings
    FROM {{ ref('base_raw_manual') }} AS raw_manual
)

SELECT
    DATE_TRUNC('month', union_data.date) AS month,
    COALESCE(op_mapping.std_operator, union_data.operator) AS operator,
    SUM(union_data.raw_earnings) AS raw_earnings
FROM union_data
LEFT JOIN {{ ref('base_raw_central_mapping') }} AS op_mapping
    ON union_data.operator = op_mapping.variants
GROUP BY 1, 2
