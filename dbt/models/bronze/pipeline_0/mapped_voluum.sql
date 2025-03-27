SELECT
    raw_voluum.date,
    raw_voluum_mapper.marketing_source,
    raw_voluum.clicks
FROM {{ ref('base_raw_voluum') }} AS raw_voluum
LEFT JOIN {{ ref('base_raw_voluum_mapper') }} AS raw_voluum_mapper
    ON raw_voluum.voluum_brand = raw_voluum_mapper.voluum_brand
