WITH marketing_sources AS (
    SELECT DISTINCT
        operator,
        marketing_source
    FROM {{ ref('base_raw_scrapers') }}
),

cross_dates AS (
    SELECT
        cal.date,
        marketing_sources.operator,
        marketing_sources.marketing_source
    FROM {{ ref('data_calendar') }} AS cal
    CROSS JOIN marketing_sources
)

SELECT
    cross_dates.date,
    cross_dates.operator,
    cross_dates.marketing_source,
    scrapers.total_earnings AS raw_total_earnings,
    COALESCE(
        scrapers.total_earnings,
        (LEAD(scrapers.total_earnings) OVER w + LAG(scrapers.total_earnings) OVER w) / 2
    ) AS total_earnings,
    scrapers.visits AS raw_visits,
    COALESCE(
        scrapers.visits,
        (LEAD(scrapers.visits) OVER w + LAG(scrapers.visits) OVER w) / 2
    ) AS visits
FROM cross_dates
LEFT JOIN {{ ref('base_raw_scrapers') }} AS scrapers
    ON cross_dates.date = scrapers.date
        AND cross_dates.operator = scrapers.operator
        AND cross_dates.marketing_source = scrapers.marketing_source
WINDOW w AS (PARTITION BY cross_dates.marketing_source, cross_dates.operator ORDER BY cross_dates.date)
