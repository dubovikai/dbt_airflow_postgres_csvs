SELECT
    COALESCE(s1.date, s2.date) AS date,
    COALESCE(s1.marketing_source, s2.marketing_source) AS marketing_source,
    s1.commission AS sol_1_commission,
    s2.commission AS sol_2_commission,
    COALESCE(s1.commission, 0) + COALESCE(s2.commission, 0) AS total_commission
FROM {{ ref('solution_1_per_source') }} AS s1
FULL JOIN {{ ref('solution_2_per_source') }} AS s2
    ON s1.date = s2.date
        AND s1.marketing_source = s2.marketing_source
