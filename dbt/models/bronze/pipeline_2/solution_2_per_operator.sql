WITH remapped_operator AS (
    SELECT
        DATE(DATE_TRUNC('month', scrappers.date)) AS month,
        COALESCE(op_mapping.std_operator, scrappers.operator) AS operator,
        MAX(scrappers.total_earnings) AS max_total_earnings
    FROM {{ ref('base_raw_scrapers') }} AS scrappers
    LEFT JOIN {{ ref('base_raw_central_mapping') }} AS op_mapping
        ON scrappers.operator = op_mapping.variants
    GROUP BY 1, 2
)

-- commission is calculated as raw_earnings 
SELECT
    month,
    operator,
    max_total_earnings - COALESCE(
        LAG(max_total_earnings) OVER (PARTITION BY operator ORDER BY month),
        0
    ) AS commission
FROM remapped_operator