WITH r_routy AS (
    SELECT
        raw_routy.date,
        raw_routy.marketing_source,
        SUM(raw_routy.raw_earnings) AS raw_earnings,
        SUM(raw_routy.visits) AS visits,
        SUM(raw_routy.signups) AS signups
    FROM {{ ref('base_raw_routy') }} AS raw_routy
    GROUP BY 1, 2
),

m_routy_voluum AS (
    SELECT
        r_routy.date,
        r_routy.marketing_source,
        r_routy.raw_earnings,
        r_routy.visits,
        r_routy.signups,
        r_voluum.clicks
    FROM r_routy
    LEFT JOIN {{ ref('mapped_voluum') }} AS r_voluum
        ON r_voluum.date = r_routy.date
            AND r_voluum.marketing_source = r_routy.marketing_source
),

r_manual AS (
    SELECT
        raw_manual.date,
        raw_manual.marketing_source,
        raw_manual.raw_earnings,
        raw_manual.visits,
        raw_manual.signups,
        NULL::int  AS clicks
    FROM {{ ref('base_raw_manual') }} AS raw_manual
),

united_data AS (
    SELECT * FROM m_routy_voluum
    UNION ALL 
    SELECT * FROM r_manual
)

SELECT
    date,
    marketing_source,
    SUM(raw_earnings) AS raw_earnings,
    SUM(visits) AS visits,
    SUM(signups) AS signups,
    COALESCE(SUM(clicks), 0) AS clicks
FROM united_data
GROUP BY 1, 2
