SELECT
    COALESCE(s1.month, s2.month) AS month,
    COALESCE(s1.operator, s2.operator) AS operator,
    s1.commission AS sol_1_commission,
    s2.commission AS sol_2_commission,
    COALESCE(s1.commission, 0) + COALESCE(s2.commission, 0) AS total_commission
FROM {{ ref('solution_1_per_operator') }} AS s1
FULL JOIN {{ ref('solution_2_per_operator') }} AS s2
    ON s1.month = s2.month
        AND s1.operator = s2.operator
