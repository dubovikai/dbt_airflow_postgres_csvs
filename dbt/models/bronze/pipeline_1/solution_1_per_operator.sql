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
    DATE_TRUNC('month', date) AS month,
    operator,
    SUM(raw_earnings) AS raw_earnings
FROM union_data
GROUP BY 1, 2
