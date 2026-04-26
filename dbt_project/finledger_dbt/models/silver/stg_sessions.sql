WITH base AS (
    SELECT *,
        ROW_NUMBER() OVER(
            PARTITION BY account_key, partition_date
            ORDER BY session_txn_count DESC
        ) AS rn
    FROM read_parquet(
        '/workspaces/FinLedger/data/bronze/sessions/*.parquet'
    )
)
SELECT
    account_key,
    session_txn_count,
    ROUND(total_amount, 2)        AS total_amount_usd,
    distinct_countries,
    max_geo_speed_kmph,
    anomaly_score,
    is_anomaly,
    anomaly_type,
    partition_date
FROM base WHERE rn = 1