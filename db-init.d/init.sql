-- Create database and user (if not already present)
CREATE DATABASE IF NOT EXISTS airflow;
-- CREATE USER IF NOT EXISTS airflow IDENTIFIED BY 'airflow';
-- GRANT ALL ON airflow.* TO airflow;

-- Now your table creation commands:
CREATE TABLE airflow.kafka_table
(
    key String,
    value String
) ENGINE = Kafka
SETTINGS kafka_broker_list = 'kafka:9092',
         kafka_topic_list = 'divar_real_estate',
         kafka_group_name = 'clickhouse_divar_group',
         kafka_format = 'JSONEachRow',
         kafka_num_consumers = 4;

CREATE TABLE airflow.divar_real_estate
(
    title String,
    metrage Decimal64(2),
    build_year UInt16,
    rooms UInt8,
    total_price Decimal64(0),
    price_per_meter Decimal64(0),
    floor String,
    location String,
    price Decimal64(0),
    url String,
    city String,
    created_at DateTime DEFAULT now(),
    source String DEFAULT 'divar'
) ENGINE = MergeTree()
PARTITION BY city
ORDER BY (location, created_at);

CREATE MATERIALIZED VIEW airflow.kafka_divar_mv TO airflow.divar_real_estate AS
SELECT
    JSONExtractString(value, 'title') as title,
    toDecimal64(JSONExtractFloat(value, 'metrage'), 2) as metrage,
    toUInt16(JSONExtractInt(value, 'build_year')) as build_year,
    toUInt8(JSONExtractInt(value, 'rooms')) as rooms,
    toDecimal64(replaceAll(replaceAll(JSONExtractString(value, 'total_price'), ' تومان', ''), ',', ''), 0) as total_price,
    toDecimal64(replaceAll(replaceAll(JSONExtractString(value, 'price_per_meter'), ' تومان', ''), ',', ''), 0) as price_per_meter,
    toUInt8(JSONExtractInt(value, 'floor')) as floor,
    JSONExtractString(value, 'location') as location,
    toDecimal64(replaceAll(replaceAll(JSONExtractString(value, 'price'), ' تومان', ''), ',', ''), 0) as price,
    JSONExtractString(value, 'url') as url,
    JSONExtractString(value, 'city') as city,
    now() as created_at,
    'divar' as source
FROM airflow.kafka_table
WHERE length(value) > 2;
