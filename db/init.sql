CREATE SCHEMA IF NOT EXISTS bronze;

CREATE TABLE IF NOT EXISTS bronze.file_load_log(
    id INTEGER PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    dataset VARCHAR NOT NULL,
    file_name VARCHAR NOT NULL,
    loaded_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS bronze.raw_manual(
    date VARCHAR,
    marketing_source VARCHAR,
    operator VARCHAR,
    raw_earnings VARCHAR,
    visits VARCHAR,
    signups VARCHAR,
    file_id INT REFERENCES bronze.file_load_log(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS bronze.raw_routy(
    date VARCHAR,
    marketing_source VARCHAR,
    operator VARCHAR,
    country VARCHAR,
    countryCode VARCHAR,
    raw_earnings VARCHAR,
    visits VARCHAR,
    signups VARCHAR,
    file_id INT REFERENCES bronze.file_load_log(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS bronze.raw_scrapers(
    date VARCHAR,
    marketing_source VARCHAR,
    operator VARCHAR,
    country VARCHAR,
    total_earnings VARCHAR,
    visits VARCHAR,
    file_id INT REFERENCES bronze.file_load_log(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS bronze.raw_voluum(
    date VARCHAR,
    voluum_brand VARCHAR,
    clicks VARCHAR,
    file_id INT REFERENCES bronze.file_load_log(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS bronze.raw_central_mapping(
    variants VARCHAR,
    std_operator VARCHAR,
    country VARCHAR,
    file_id INT REFERENCES bronze.file_load_log(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS bronze.raw_deals(
    marketing_source VARCHAR,
    deal VARCHAR,
    comments VARCHAR,
    file_id INT REFERENCES bronze.file_load_log(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS bronze.raw_voluum_mapper(
    voluum_brand VARCHAR,
    marketing_source VARCHAR,
    file_id INT REFERENCES bronze.file_load_log(id) ON DELETE CASCADE
);
