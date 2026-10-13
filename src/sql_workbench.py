"""
Interactive SQL Operational Warehouse & Reconciliation Workbench.
Loads staging (Bronze) and cleansed (Silver/Gold) tables into SQLite and
provides pre-built operational reconciliation, SLA, and compliance audit queries.
"""

from __future__ import annotations

import sqlite3
from typing import Dict, Tuple
import pandas as pd


PREBUILT_SQL_QUERIES: Dict[str, Dict[str, str]] = {
    "1. Order-to-Payment Ledger Reconciliation (Variance > $0.05)": {
        "category": "Financial Reconciliation",
        "description": "Identifies orders where the payment gateway captured amount deviates from the net order invoice total.",
        "sql": """WITH UniqueOrders AS (
    SELECT order_id, customer_id, merchant_region, net_order_total_usd, order_status
    FROM orders_raw
    GROUP BY order_id
),
UniquePayments AS (
    SELECT payment_id, order_id, gateway_provider, captured_amount_usd, payment_status
    FROM payments_raw
    GROUP BY payment_id
)
SELECT 
    o.order_id,
    p.payment_id,
    o.merchant_region,
    p.gateway_provider,
    ROUND(o.net_order_total_usd, 2) AS invoice_total_usd,
    ROUND(p.captured_amount_usd, 2) AS gateway_captured_usd,
    ROUND(p.captured_amount_usd - o.net_order_total_usd, 2) AS ledger_variance_usd,
    p.payment_status
FROM UniqueOrders o
INNER JOIN UniquePayments p ON o.order_id = p.order_id
WHERE ABS(p.captured_amount_usd - o.net_order_total_usd) > 0.05
ORDER BY ABS(p.captured_amount_usd - o.net_order_total_usd) DESC
LIMIT 50;""",
    },
    "2. Orphan Payment Captures (CDC Replication Lag Audit)": {
        "category": "Referential Integrity",
        "description": "Detects payment captures in payments_raw whose parent order_id is missing from orders_raw due to upstream CDC delay.",
        "sql": """SELECT 
    p.payment_id,
    p.order_id AS missing_parent_order_id,
    p.customer_id,
    p.gateway_provider,
    p.payment_method,
    ROUND(p.captured_amount_usd, 2) AS unlinked_capture_usd,
    p.payment_timestamp
FROM payments_raw p
LEFT JOIN orders_raw o ON p.order_id = o.order_id
WHERE o.order_id IS NULL
ORDER BY p.captured_amount_usd DESC;""",
    },
    "3. Webhook Retry Storm & Duplicate PK Detection (Window Function)": {
        "category": "Data Cleansing & Deduplication",
        "description": "Uses ROW_NUMBER() window logic to isolate duplicate order_id records ingested during gateway retry storms.",
        "sql": """WITH RankedOrders AS (
    SELECT 
        order_id,
        customer_id,
        source_system,
        ingestion_batch_id,
        net_order_total_usd,
        order_timestamp,
        ROW_NUMBER() OVER (
            PARTITION BY order_id 
            ORDER BY order_timestamp ASC
        ) AS duplicate_rank,
        COUNT(*) OVER (PARTITION BY order_id) AS total_occurrences
    FROM orders_raw
)
SELECT 
    order_id,
    customer_id,
    source_system,
    ingestion_batch_id,
    ROUND(net_order_total_usd, 2) AS duplicate_exposure_usd,
    duplicate_rank,
    total_occurrences
FROM RankedOrders
WHERE duplicate_rank > 1
ORDER BY duplicate_exposure_usd DESC;""",
    },
    "4. High-Value AML/KYC Compliance Breach Audit": {
        "category": "Compliance & Risk",
        "description": "Audits orders where UNVERIFIED or EXPIRED KYC customers transacted above their tier limit or hold AML scores > 85.",
        "sql": """SELECT 
    o.order_id,
    o.customer_id,
    k.account_tier,
    k.kyc_verification_status,
    k.aml_risk_score,
    k.country_code,
    ROUND(k.max_single_tx_limit_usd, 2) AS tier_tx_limit_usd,
    ROUND(o.net_order_total_usd, 2) AS order_total_usd,
    ROUND(o.net_order_total_usd - k.max_single_tx_limit_usd, 2) AS limit_excess_usd
FROM orders_raw o
INNER JOIN customer_kyc_raw k ON o.customer_id = k.customer_id
WHERE (k.kyc_verification_status IN ('UNVERIFIED', 'EXPIRED') 
       AND o.net_order_total_usd > k.max_single_tx_limit_usd)
   OR k.aml_risk_score > 85.0
GROUP BY o.order_id
ORDER BY o.net_order_total_usd DESC
LIMIT 50;""",
    },
    "5. Over-Refund Revenue Leakage Detection": {
        "category": "Revenue Assurance",
        "description": "Finds refund transactions where the refunded amount exceeds the original payment capture.",
        "sql": """SELECT 
    r.refund_id,
    r.payment_id,
    r.order_id,
    r.refund_reason,
    ROUND(p.captured_amount_usd, 2) AS original_captured_usd,
    ROUND(r.refund_amount_usd, 2) AS refund_issued_usd,
    ROUND(r.refund_amount_usd - ABS(p.captured_amount_usd), 2) AS revenue_leakage_usd,
    r.refund_status
FROM refunds_raw r
INNER JOIN payments_raw p ON r.payment_id = p.payment_id
WHERE r.refund_amount_usd > ABS(p.captured_amount_usd)
GROUP BY r.refund_id
ORDER BY revenue_leakage_usd DESC;""",
    },
    "6. Payment Gateway Settlement SLA Breach Summary": {
        "category": "Operational SLA",
        "description": "Aggregates settlement latency and SLA breach rates across payment gateways and methods.",
        "sql": """SELECT 
    gateway_provider,
    payment_method,
    COUNT(*) AS total_transactions,
    SUM(CASE WHEN actual_settlement_hours > settlement_sla_hours THEN 1 ELSE 0 END) AS sla_breach_count,
    ROUND(
        100.0 * SUM(CASE WHEN actual_settlement_hours > settlement_sla_hours THEN 1 ELSE 0 END) / COUNT(*),
        2
    ) AS sla_breach_rate_pct,
    ROUND(AVG(actual_settlement_hours), 2) AS avg_settlement_hrs,
    ROUND(MAX(actual_settlement_hours), 2) AS max_settlement_hrs
FROM payments_raw
GROUP BY gateway_provider, payment_method
ORDER BY sla_breach_rate_pct DESC;""",
    },
    "7. Enterprise Scheduler Pipeline Reliability & Quarantine KPI": {
        "category": "Pipeline Observability",
        "description": "Summarizes DAG execution duration, SLA compliance, and quarantine rates across Apache Airflow, Control-M, and Databricks.",
        "sql": """SELECT 
    pipeline_name,
    scheduler_engine,
    upstream_source,
    COUNT(*) AS total_runs,
    SUM(CASE WHEN sla_breached = 1 THEN 1 ELSE 0 END) AS sla_breaches,
    ROUND(AVG(duration_sec), 1) AS avg_duration_sec,
    MAX(sla_threshold_sec) AS sla_limit_sec,
    SUM(records_ingested) AS total_ingested,
    SUM(records_quarantined) AS total_quarantined,
    ROUND(100.0 * SUM(records_quarantined) / SUM(records_ingested), 2) AS quarantine_rate_pct
FROM pipeline_runs
GROUP BY pipeline_name, scheduler_engine, upstream_source
ORDER BY quarantine_rate_pct DESC;""",
    },
    "8. Bronze vs. Silver Layer Cleansing Impact Reconciliation": {
        "category": "ETL Layer Audit",
        "description": "Compares record counts and net USD volume between raw staging (Bronze) and validated production (Silver) tables.",
        "sql": """SELECT 
    'Orders Feed' AS pipeline_feed,
    (SELECT COUNT(*) FROM orders_raw) AS bronze_raw_rows,
    (SELECT COUNT(*) FROM orders_clean) AS silver_clean_rows,
    (SELECT COUNT(*) FROM orders_raw) - (SELECT COUNT(*) FROM orders_clean) AS rows_removed_or_quarantined,
    ROUND((SELECT SUM(net_order_total_usd) FROM orders_clean), 2) AS verified_clean_volume_usd
UNION ALL
SELECT 
    'Payments Feed' AS pipeline_feed,
    (SELECT COUNT(*) FROM payments_raw) AS bronze_raw_rows,
    (SELECT COUNT(*) FROM payments_clean) AS silver_clean_rows,
    (SELECT COUNT(*) FROM payments_raw) - (SELECT COUNT(*) FROM payments_clean) AS rows_removed_or_quarantined,
    ROUND((SELECT SUM(captured_amount_usd) FROM payments_clean), 2) AS verified_clean_volume_usd;""",
    },
}


def build_sqlite_warehouse(
    raw_datasets: Dict[str, pd.DataFrame],
    validation_output: Dict[str, pd.DataFrame],
) -> sqlite3.Connection:
    """
    Create an in-memory SQLite relational database populated with both
    Bronze (raw) and Silver (cleansed) operational tables.
    """
    conn = sqlite3.connect(":memory:", check_same_thread=False)

    for tbl_name, df in raw_datasets.items():
        df.to_sql(tbl_name, conn, index=False, if_exists="replace")

    for tbl_name, df in validation_output["cleansed_datasets"].items():
        df.to_sql(tbl_name, conn, index=False, if_exists="replace")

    validation_output["quarantine_dlq"].to_sql(
        "quarantine_dlq", conn, index=False, if_exists="replace"
    )
    validation_output["rule_results"].to_sql(
        "dq_rule_results", conn, index=False, if_exists="replace"
    )

    return conn


def execute_sql_query(
    conn: sqlite3.Connection, sql_text: str
) -> Tuple[pd.DataFrame | None, str | None]:
    """
    Safely execute a read-only SQL query against the SQLite warehouse
    and return (result_df, error_message).
    """
    forbidden = ["DROP ", "DELETE ", "TRUNCATE ", "ALTER ", "UPDATE ", "INSERT "]
    upper_sql = sql_text.upper()
    for token in forbidden:
        if token in upper_sql:
            return None, f"Security Policy: Read-only SQL workbench blocks '{token.strip()}' statements."

    try:
        df = pd.read_sql_query(sql_text, conn)
        return df, None
    except Exception as exc:
        return None, str(exc)
