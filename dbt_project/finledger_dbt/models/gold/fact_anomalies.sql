SELECT
    account_key,
    partition_date,
    COUNT(*)                                    AS total_sessions,
    SUM(CASE WHEN is_anomaly THEN 1 ELSE 0 END) AS flagged_sessions,
    ROUND(
        100.0 * SUM(CASE WHEN is_anomaly THEN 1 ELSE 0 END) / COUNT(*), 2
    )                                           AS anomaly_rate_pct,
    MAX(anomaly_score)                          AS max_anomaly_score,
    SUM(total_amount_usd)                       AS total_volume_usd
FROM {{ ref('stg_sessions') }}
GROUP BY account_key, partition_date
