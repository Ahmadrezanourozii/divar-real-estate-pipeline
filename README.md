# Divar Real-Estate Data Pipeline

Course project for **Advanced Database Systems** (February 2025): an end-to-end pipeline
that collects apartment-for-sale listings from [Divar](https://divar.ir), Iran's largest
classifieds site, streams them through Kafka into ClickHouse and makes them queryable in Metabase.
Everything runs with one `docker compose` command.

```
              Airflow (every 30 min)
 ┌──────────────────────────────────────────────────────┐
 │  scrape  ──►  read & merge  ──►  clean  ──►  produce │──► Kafka ──► consumer ──► ClickHouse ──► Metabase
 │ (Selenium)    (3 cities)       (Persian    (one msg   │    topic                 MergeTree      dashboards
 │                                 numerals)   per ad)   │                          by city
 └──────────────────────────────────────────────────────┘
```

## Components

| Service | Role |
|---|---|
| `scraper/divar_scraper.py` | Headless Chrome (Selenium) walks the *buy-apartment* listings of Tehran, Mashhad and Isfahan, opens each ad and extracts area, build year, rooms, total price, price per m², floor and neighbourhood; checkpoints to Excel every *n* ads |
| `dags/real_estate_dag.py` | Airflow DAG, scheduled every 30 minutes: scrape → merge the city files → clean → publish to Kafka |
| `dags/migrate_already_crawled_data.py` | One-off DAG that loads the earlier crawl (≈ 6,000 ads in `data/`) into the same pipeline on its own topic |
| `kafka_producer/producer.py` | Thin wrapper around `confluent_kafka` with `acks=all` and exponential-backoff retries |
| `kafka_consumer/kafka_clickhouse_consumer.py` | Standalone container that consumes both topics and inserts rows into ClickHouse, restarting itself after a crash |
| `db-init.d/init.sql` | ClickHouse schema: `divar_real_estate` (MergeTree, partitioned by city, ordered by neighbourhood and time), plus a Kafka-engine table and materialized view as an alternative ingestion path |
| Metabase | *Divar* dashboard with SQL questions on ClickHouse, e.g. the ten neighbourhoods with the most listings above 100 m² |
| Kafka UI | Topic and consumer-lag inspection on port 7777 |

## Cleaning

Divar shows numbers in Persian digits with currency words and thousands separators
(`۲۰٬۰۰۰٬۰۰۰٬۰۰۰ تومان`). `persian_to_uint` in the DAG maps Persian digits to ASCII,
strips `تومان`, `قبل از` and both separator characters, and turns *free* / *no room*
into 0, so that price, price per m², rooms and build year can be stored as integers.
Rows with missing fields are dropped.

From the initial crawl of 5,950 ads, 4,562 complete listings remained. Median values:

| City | Price per m² (toman) | Area (m²) | Build year (Solar Hijri) |
|---|---|---|---|
| Tehran | 96.4 M | 92 | 1396 |
| Isfahan | 44.0 M | 115 | 1399 |
| Mashhad | 33.8 M | 105 | 1400 |

## Running it

```bash
docker compose up --build -d
```

| URL | Service | Login |
|---|---|---|
| http://localhost:8080 | Airflow | airflow / airflow |
| http://localhost:3000 | Metabase | set up on first visit |
| http://localhost:7777 | Kafka UI | — |

Metabase needs the ClickHouse driver: download `clickhouse.metabase-driver.jar` from the
[ClickHouse Metabase driver releases](https://github.com/ClickHouse/metabase-clickhouse-driver/releases)
into `clickhouse_jdbc/` before starting. Then add the database in Metabase:

- Database type: ClickHouse
- Host: `clickhouse`, port `8123`
- Database name: `airflow`
- Username / password: `airflow` / `airflow`

In Airflow, trigger `migrate_already_crawled_data_pipeline` once to load the earlier crawl,
then enable `real_estate_pipeline` for the scheduled runs.

The images are pulled from the `registry.docker.ir` mirror, which was needed in Iran; replace
the prefix with Docker Hub elsewhere. The credentials are local development defaults only.
