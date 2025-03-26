{% macro commission_case_stmnt() %}
    {% set commission_calc_query %}
    SELECT
        marketing_source,
        calc_formula
    FROM {{ ref('base_raw_deals') }}
    {% endset %}

    {% set results = run_query(commission_calc_query) %}

    {% if execute %}
        {% set results_list = results %}
    {% else %}
        {% set results_list = [] %}
    {% endif %}

    CASE marketing_source
    {% for row in results_list.rows if row['marketing_source'] != 'Default' %}
        WHEN '{{ row['marketing_source'] }}' THEN {{ row['calc_formula'] }}
    {% endfor %}
        ELSE {{ results_list.rows | selectattr("marketing_source", "equalto", 'Default') | map(attribute='calc_formula') | first }}
    END

{% endmacro %}