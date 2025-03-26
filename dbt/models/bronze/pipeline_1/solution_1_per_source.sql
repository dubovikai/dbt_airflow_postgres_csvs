
WITH calcs AS (
    SELECT
        date,
        marketing_source,
        raw_earnings,
        visits,
        signups,
        clicks,
        {{ commission_case_stmnt() }} AS commission
    FROM {{ ref('merged_data_per_source') }}
)

SELECT
    date,
    marketing_source,
    raw_earnings,
    visits,
    signups,
    clicks,
    CASE WHEN commission < 0 THEN 0
    WHEN commission IS NULL THEN 
        raw_earnings
    ELSE
        commission
    END AS commission
FROM calcs
