# FinLedger — Regulatory-Grade Financial Data Pipeline

## Architecture
Postgres → Debezium CDC → Kafka → Flink Session Windowing → MinIO (Parquet) → dbt (Silver/Gold) → DuckDB Warehouse → Grafana Dashboard

Orchestrated by Apache Airflow | Quality by Great Expectations

## What it does
End-to-end ELT pipeline that ingests raw transaction logs via CDC, enforces 
PII masking at ingest (SHA-256 tokenisation), scores sessions for fraud using 
Z-score velocity detection and haversine geo-jump analysis, models a snowflake 
schema for regulatory reporting, and validates every layer with Great Expectations 
quality contracts.

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

## Key Concepts
- CDC ingestion via Debezium — captures every INSERT/UPDATE/DELETE with sub-200ms latency
- Flink session windowing — groups transactions by user inactivity, not clock time
- Z-score + haversine anomaly scoring — flags velocity bursts and impossible geo-travel
- SHA-256 PII tokenisation — real account IDs never enter the data lake
- Great Expectations contracts — pipeline halts automatically on schema or quality breach
- Airflow DAG — 7-stage orchestration with retry logic

## How to Run
```bash
git clone https://github.com/abhinavwalde204/FinLedger
cd FinLedger/docker
docker-compose up -d
python datagen/generate_transactions.py
python flink_jobs/session_windowing.py
```

Open in browser:
- MinIO dashboard: port 9001
- Grafana dashboard: port 3000
- Airflow: port 8080

## Screenshots

- [Airflow Orchestration]<img width="1919" height="869" alt="Screenshot 2026-04-28 124831" src="https://github.com/user-attachments/assets/97114c37-12d9-4067-9e48-5471b22abdff" />


- [Total transaction shown in Grafana]<img width="1919" height="873" alt="Screenshot 2026-04-28 125329" src="https://github.com/user-attachments/assets/094c47ae-fae2-4109-b79d-ab4822830d20" />


- [Representaion of Schema]<img width="687" height="314" alt="Screenshot 2026-04-28 130120" src="https://github.com/user-attachments/assets/b3464c32-ec87-45c7-b93f-9ec94602edb4" />


- [Flagged Sessions]<img width="858" height="549" alt="Screenshot 2026-04-28 130445" src="https://github.com/user-attachments/assets/06e6ec7f-aafb-4e17-b631-fb1ca93b4460" />
