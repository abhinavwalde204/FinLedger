# FinLedger — Regulatory-Grade Financial Data Pipeline

## Architecture
Postgres → Debezium CDC → Kafka → Flink Session Windowing → MinIO (Parquet) → dbt (Silver/Gold) → DuckDB Warehouse → Grafana Dashboard

Orchestrated by Apache Airflow | Quality by Great Expectations

## Stack
| Layer | Tool |
|-------|------|
| Source DB | PostgreSQL 14 |
| CDC | Debezium 2.4 |
| Message Bus | Apache Kafka |
| Stream Processing | Apache Flink 1.18 |
| Data Lake | MinIO + Parquet |
| Transformation | dbt Core |
| Warehouse | DuckDB |
| Orchestration | Apache Airflow |
| Data Quality | Great Expectations |
| Monitoring | Grafana |

## Key Concepts Demonstrated
- SCD Type 2 account dimension with dbt snapshots
- Flink session windowing for fraud velocity and geo-jump detection
- PII masking via SHA-256 tokenisation at ingest
- Great Expectations quality contracts halt pipeline on breach
- Column-level data lineage tracking for GDPR compliance
- Airflow DAG with retry logic and 7-stage orchestration

## How to Run
```bash
git clone https://github.com/abhinavwalde204/FinLedger
cd FinLedger/docker
docker-compose up -d
python datagen/generate_transactions.py &
python flink_jobs/session_windowing.py
```

Open ports: MinIO(9001) Grafana(3000) Airflow(8080)

## Resume Bullets
- Built end-to-end ELT pipeline ingesting CDC events from PostgreSQL via Debezium, processing 500+ accounts through Apache Flink session windows with real-time fraud velocity scoring and geo-jump anomaly detection
- Designed dbt snowflake schema with automated PII masking (SHA-256) and Great Expectations quality contracts halting pipeline on breach
- Deployed Grafana monitoring dashboard tracking live transaction volume and anomaly rates across 37,000+ transactions