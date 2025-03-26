WITH aggregated AS (
    SELECT
        date,
        marketing_source,
        SUM(total_earnings) AS total_earnings,
        SUM(visits) AS visits
    FROM {{ ref('base_raw_scrapers') }}
    GROUP BY 1, 2
),

calc AS (
    SELECT
        date,
        marketing_source,
        total_earnings - LAG(total_earnings) OVER (PARTITION BY marketing_source ORDER BY date) AS raw_earnings,
        visits - LAG(visits) OVER (PARTITION BY marketing_source ORDER BY date) AS visits
    FROM aggregated
),

clicks AS (
    SELECT
        calc.date,
        calc.marketing_source,
        CASE WHEN calc.raw_earnings < 0 OR calc.raw_earnings IS NULL
        THEN 0 ELSE calc.raw_earnings END AS raw_earnings,
        CASE WHEN calc.visits < 0 OR calc.visits IS NULL
        THEN 0 ELSE calc.visits END AS visits,
        r_voluum.clicks
    FROM calc
    LEFT JOIN {{ ref('mapped_voluum') }} AS r_voluum
        ON r_voluum.date = calc.date
            AND r_voluum.marketing_source = calc.marketing_source
),

commission AS (
    SELECT
        date,
        marketing_source,
        raw_earnings,
        visits,
        clicks,
        {{ commission_case_stmnt() }} AS commission
    FROM clicks
)

SELECT
    date,
    marketing_source,
    raw_earnings,
    visits,
    clicks,
    CASE WHEN commission < 0 THEN 0
    WHEN commission IS NULL THEN 
        raw_earnings
    ELSE
        commission
    END AS commission
FROM commission
