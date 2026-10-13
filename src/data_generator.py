"""
Enterprise FinTech & E-Commerce Order-to-Cash (O2C) Synthetic Data Generator.
Simulates multi-source ETL/ELT feeds with realistic production anomalies:
- Duplicate primary keys (webhook retry storms)
- Schema & constraint violations (negative amounts, invalid currencies, future dates)
- Referential integrity breaks (orphan payments, missing KYC records)
- Compliance breaches (unverified KYC accounts exceeding transaction thresholds)
- Enterprise Job Scheduler SLA & latency telemetry
"""

from __future__ import annotations

import datetime as dt
from typing import Dict
import numpy as np
import pandas as pd


SCENARIO_CONFIGS = {
    "Standard Production Operations (Baseline)": {
        "dup_rate": 0.035,
        "null_rate": 0.03,
        "negative_amt_rate": 0.018,
        "orphan_payment_rate": 0.025,
        "kyc_breach_rate": 0.03,
        "over_refund_rate": 0.08,
        "sla_breach_boost": 1.0,
        "description": "Normal day-to-day enterprise Order-to-Cash feeds with routine data quality exceptions (~6.5% total quarantine rate).",
    },
    "P1 Incident: Payment Webhook Retry Storm": {
        "dup_rate": 0.115,
        "null_rate": 0.025,
        "negative_amt_rate": 0.015,
        "orphan_payment_rate": 0.065,
        "kyc_breach_rate": 0.025,
        "over_refund_rate": 0.12,
        "sla_breach_boost": 1.8,
        "description": "Simulates an upstream Adyen/Stripe webhook retry storm causing duplicate order/payment ingestion and CDC replication lag.",
    },
    "P1 Incident: Upstream ERP Schema Drift & Null Spike": {
        "dup_rate": 0.03,
        "null_rate": 0.11,
        "negative_amt_rate": 0.045,
        "orphan_payment_rate": 0.03,
        "kyc_breach_rate": 0.05,
        "over_refund_rate": 0.07,
        "sla_breach_boost": 1.4,
        "description": "Simulates an unannounced SAP/Salesforce payload update dropping mandatory region/customer fields and introducing currency code drift.",
    },
    "P2 Incident: AML/KYC Compliance Sync Outage": {
        "dup_rate": 0.025,
        "null_rate": 0.025,
        "negative_amt_rate": 0.015,
        "orphan_payment_rate": 0.025,
        "kyc_breach_rate": 0.12,
        "over_refund_rate": 0.09,
        "sla_breach_boost": 1.3,
        "description": "Simulates a stalled KYC microservice batch job allowing unverified/expired accounts to execute high-value wire settlements.",
    },
}


def generate_o2c_datasets(
    n_orders: int = 1200,
    scenario: str = "Standard Production Operations (Baseline)",
    seed: int = 42,
) -> Dict[str, pd.DataFrame]:
    """
    Generate interconnected raw staging datasets for Order-to-Cash (O2C) pipelines:
    - customer_kyc_raw
    - orders_raw
    - payments_raw
    - refunds_raw
    - pipeline_runs
    """
    rng = np.random.default_rng(seed)
    cfg = SCENARIO_CONFIGS.get(
        scenario, SCENARIO_CONFIGS["Standard Production Operations (Baseline)"]
    )

    base_date = dt.datetime(2026, 10, 1, 0, 0, 0)

    # -------------------------------------------------------------------------
    # 1. CUSTOMER KYC & COMPLIANCE REGISTRY (customer_kyc_raw)
    # -------------------------------------------------------------------------
    n_customers = max(300, int(n_orders * 0.45))
    customer_ids = [f"CUST-{10000 + i}" for i in range(n_customers)]
    tiers = rng.choice(
        ["ENTERPRISE", "MID_MARKET", "SMB", "RETAIL"],
        size=n_customers,
        p=[0.18, 0.27, 0.35, 0.20],
    )
    tier_limits = {
        "ENTERPRISE": 50000.0,
        "MID_MARKET": 15000.0,
        "SMB": 5000.0,
        "RETAIL": 2000.0,
    }
    max_limits = [tier_limits[t] for t in tiers]

    kyc_probs = [0.82, 0.08, 0.06, 0.04]
    if cfg["kyc_breach_rate"] > 0.06:
        kyc_probs = [0.64, 0.16, 0.12, 0.08]

    kyc_statuses = rng.choice(
        ["VERIFIED", "PENDING_REVIEW", "EXPIRED", "UNVERIFIED"],
        size=n_customers,
        p=kyc_probs,
    )
    countries = rng.choice(
        ["US", "IN", "GB", "DE", "SG", "CA", "AE", "AU"],
        size=n_customers,
        p=[0.32, 0.22, 0.14, 0.09, 0.08, 0.06, 0.05, 0.04],
    )
    aml_scores = np.clip(rng.normal(loc=32, scale=22, size=n_customers), 5, 99).round(1)
    audit_days_ago = rng.integers(1, 420, size=n_customers)
    audit_dates = [
        (base_date - dt.timedelta(days=int(d))).strftime("%Y-%m-%d")
        for d in audit_days_ago
    ]

    kyc_df = pd.DataFrame(
        {
            "customer_id": customer_ids,
            "account_tier": tiers,
            "kyc_verification_status": kyc_statuses,
            "aml_risk_score": aml_scores,
            "country_code": countries,
            "max_single_tx_limit_usd": max_limits,
            "last_kyc_audit_date": audit_dates,
        }
    )

    # Inject missing country codes in KYC
    missing_cc_idx = rng.choice(
        kyc_df.index, size=max(3, int(len(kyc_df) * 0.02)), replace=False
    )
    kyc_df.loc[missing_cc_idx, "country_code"] = None

    # -------------------------------------------------------------------------
    # 2. ORDERS FEED (orders_raw)
    # -------------------------------------------------------------------------
    order_ids = [f"ORD-2026-{800000 + i}" for i in range(n_orders)]
    sampled_customers = rng.choice(customer_ids, size=n_orders, replace=True)
    regions = rng.choice(
        ["NA-EAST", "NA-WEST", "APAC-IN", "EMEA-EU", "APAC-SG", "LATAM"],
        size=n_orders,
        p=[0.28, 0.20, 0.22, 0.16, 0.09, 0.05],
    )
    categories = rng.choice(
        [
            "Cloud Infrastructure",
            "Enterprise SaaS License",
            "API Metering Bundle",
            "Data Analytics Add-On",
            "Managed Security Suite",
        ],
        size=n_orders,
    )

    # Base order amounts
    raw_amounts = np.round(rng.lognormal(mean=6.4, sigma=1.05, size=n_orders), 2)
    raw_amounts = np.clip(raw_amounts, 45.0, 68000.0)
    tax_amounts = np.round(raw_amounts * rng.choice([0.08, 0.12, 0.18], size=n_orders), 2)
    discounts = np.round(
        raw_amounts * rng.choice([0.0, 0.05, 0.10, 0.15], size=n_orders, p=[0.55, 0.25, 0.15, 0.05]),
        2,
    )
    net_order_amounts = np.round(raw_amounts + tax_amounts - discounts, 2)

    currencies = rng.choice(
        ["USD", "EUR", "INR", "GBP", "SGD"],
        size=n_orders,
        p=[0.58, 0.15, 0.14, 0.08, 0.05],
    )
    order_statuses = rng.choice(
        ["COMPLETED", "CAPTURED", "PROCESSING", "CANCELLED", "ON_HOLD"],
        size=n_orders,
        p=[0.54, 0.24, 0.10, 0.07, 0.05],
    )
    batch_ids = [
        f"BATCH-202610{rng.integers(1, 14):02d}-{rng.integers(1, 5):02d}"
        for _ in range(n_orders)
    ]
    sources = rng.choice(
        ["SAP_ERP_CDC", "SALESFORCE_CPQ", "SHOPIFY_PLUS_API", "STRIPE_CHECKOUT"],
        size=n_orders,
        p=[0.32, 0.28, 0.22, 0.18],
    )

    minute_offsets = rng.integers(0, 13 * 24 * 60, size=n_orders)
    order_timestamps = [
        (base_date + dt.timedelta(minutes=int(m))).strftime("%Y-%m-%d %H:%M:%S")
        for m in np.sort(minute_offsets)
    ]

    orders_df = pd.DataFrame(
        {
            "order_id": order_ids,
            "customer_id": sampled_customers,
            "merchant_region": regions,
            "product_category": categories,
            "order_amount_usd": raw_amounts,
            "tax_amount_usd": tax_amounts,
            "discount_usd": discounts,
            "net_order_total_usd": net_order_amounts,
            "currency": currencies,
            "order_status": order_statuses,
            "order_timestamp": order_timestamps,
            "ingestion_batch_id": batch_ids,
            "source_system": sources,
        }
    )

    # --- Inject Order Anomalies ---
    # A. Null customer_id or merchant_region
    n_nulls = max(6, int(n_orders * cfg["null_rate"]))
    null_cust_idx = rng.choice(orders_df.index, size=n_nulls // 2, replace=False)
    null_reg_idx = rng.choice(
        orders_df.index.difference(null_cust_idx), size=n_nulls - (n_nulls // 2), replace=False
    )
    orders_df.loc[null_cust_idx, "customer_id"] = None
    orders_df.loc[null_reg_idx, "merchant_region"] = None

    # B. Negative order amounts or discount > order_amount
    n_neg = max(5, int(n_orders * cfg["negative_amt_rate"]))
    neg_idx = rng.choice(orders_df.index, size=n_neg, replace=False)
    half_neg = n_neg // 2
    orders_df.loc[neg_idx[:half_neg], "order_amount_usd"] = -np.abs(
        orders_df.loc[neg_idx[:half_neg], "order_amount_usd"]
    )
    orders_df.loc[neg_idx[:half_neg], "net_order_total_usd"] = (
        orders_df.loc[neg_idx[:half_neg], "order_amount_usd"]
        + orders_df.loc[neg_idx[:half_neg], "tax_amount_usd"]
    )
    # Discount exceeding order amount
    orders_df.loc[neg_idx[half_neg:], "discount_usd"] = (
        orders_df.loc[neg_idx[half_neg:], "order_amount_usd"] * 1.35
    ).round(2)
    orders_df.loc[neg_idx[half_neg:], "net_order_total_usd"] = (
        orders_df.loc[neg_idx[half_neg:], "order_amount_usd"]
        + orders_df.loc[neg_idx[half_neg:], "tax_amount_usd"]
        - orders_df.loc[neg_idx[half_neg:], "discount_usd"]
    ).round(2)

    # C. Schema Drift: Unsupported Currency Codes & Future Timestamps
    n_schema_drift = max(5, int(n_orders * (cfg["null_rate"] * 0.45)))
    drift_idx = rng.choice(orders_df.index, size=n_schema_drift, replace=False)
    orders_df.loc[drift_idx[: n_schema_drift // 2], "currency"] = "UNK_CRYPTO"
    orders_df.loc[drift_idx[n_schema_drift // 2 :], "order_timestamp"] = "2029-12-31 23:59:59"

    # D. KYC High-Value Breach injection (link unverified/expired customers to high-value orders)
    unverified_custs = kyc_df[
        kyc_df["kyc_verification_status"].isin(["UNVERIFIED", "EXPIRED"])
    ]["customer_id"].tolist()
    if unverified_custs:
        n_kyc_breach = max(6, int(n_orders * cfg["kyc_breach_rate"]))
        kyc_breach_idx = rng.choice(orders_df.index, size=n_kyc_breach, replace=False)
        orders_df.loc[kyc_breach_idx, "customer_id"] = rng.choice(
            unverified_custs, size=n_kyc_breach, replace=True
        )
        orders_df.loc[kyc_breach_idx, "order_amount_usd"] = np.round(
            rng.uniform(18000.0, 48000.0, size=n_kyc_breach), 2
        )
        orders_df.loc[kyc_breach_idx, "net_order_total_usd"] = np.round(
            orders_df.loc[kyc_breach_idx, "order_amount_usd"] * 1.08, 2
        )

    # E. Duplicate Order PK rows (Webhook retry storm)
    n_dups = max(8, int(n_orders * cfg["dup_rate"]))
    dup_rows = orders_df.sample(n=n_dups, random_state=seed).copy()
    dup_rows["ingestion_batch_id"] = "BATCH-20261013-RETRY"
    orders_df = pd.concat([orders_df, dup_rows], ignore_index=True)

    # -------------------------------------------------------------------------
    # 3. PAYMENTS GATEWAY FEED (payments_raw)
    # -------------------------------------------------------------------------
    valid_orders_sample = orders_df.drop_duplicates(subset=["order_id"]).sample(
        frac=0.92, random_state=seed
    )
    n_payments = len(valid_orders_sample)
    payment_ids = [f"PAY-90{10000 + i}" for i in range(n_payments)]
    gateways = rng.choice(
        ["STRIPE_GLOBAL", "ADYEN_ENTERPRISE", "RAZORPAY_CORP", "JPM_WIRE_API"],
        size=n_payments,
        p=[0.38, 0.30, 0.18, 0.14],
    )
    pay_methods = rng.choice(
        ["CORPORATE_ACH", "WIRE_TRANSFER", "CREDIT_CARD", "UPI_ENTERPRISE"],
        size=n_payments,
        p=[0.34, 0.26, 0.28, 0.12],
    )

    captured_amounts = valid_orders_sample["net_order_total_usd"].values.copy()
    fees = np.round(np.abs(captured_amounts) * 0.022, 2)
    sla_hours = np.where(pay_methods == "WIRE_TRANSFER", 24.0, 4.0)
    actual_hours = np.round(
        np.clip(
            rng.gamma(shape=2.1, scale=1.5 * cfg["sla_breach_boost"], size=n_payments),
            0.2,
            72.0,
        ),
        2,
    )
    pay_statuses = rng.choice(
        ["SETTLED", "CAPTURED", "PENDING_SETTLEMENT", "FAILED"],
        size=n_payments,
        p=[0.66, 0.20, 0.08, 0.06],
    )
    retry_counts = rng.choice([0, 1, 2, 4, 6], size=n_payments, p=[0.76, 0.12, 0.06, 0.04, 0.02])

    payments_df = pd.DataFrame(
        {
            "payment_id": payment_ids,
            "order_id": valid_orders_sample["order_id"].values,
            "customer_id": valid_orders_sample["customer_id"].values,
            "gateway_provider": gateways,
            "payment_method": pay_methods,
            "captured_amount_usd": captured_amounts,
            "processing_fee_usd": fees,
            "payment_status": pay_statuses,
            "settlement_sla_hours": sla_hours,
            "actual_settlement_hours": actual_hours,
            "webhook_retry_count": retry_counts,
            "payment_timestamp": valid_orders_sample["order_timestamp"].values,
        }
    )

    # --- Inject Payment Anomalies ---
    # A. Orphan Payments (order_id does not exist in orders_raw due to CDC lag)
    n_orphans = max(6, int(n_payments * cfg["orphan_payment_rate"]))
    orphan_idx = rng.choice(payments_df.index, size=n_orphans, replace=False)
    payments_df.loc[orphan_idx, "order_id"] = [
        f"ORD-ORPHAN-{9900 + i}" for i in range(n_orphans)
    ]

    # B. Settlement Reconciliation Variance (captured_amount != net_order_total_usd)
    n_variance = max(10, int(n_payments * 0.04))
    var_idx = rng.choice(
        payments_df.index.difference(orphan_idx), size=n_variance, replace=False
    )
    payments_df.loc[var_idx, "captured_amount_usd"] = np.round(
        payments_df.loc[var_idx, "captured_amount_usd"]
        * rng.choice([0.72, 0.85, 1.18, 1.40], size=n_variance),
        2,
    )

    # C. Duplicate Payment Webhook deliveries
    n_pay_dups = max(6, int(n_payments * (cfg["dup_rate"] * 0.7)))
    pay_dup_rows = payments_df.sample(n=n_pay_dups, random_state=seed + 7).copy()
    pay_dup_rows["webhook_retry_count"] = 5
    payments_df = pd.concat([payments_df, pay_dup_rows], ignore_index=True)

    # -------------------------------------------------------------------------
    # 4. REFUNDS & CHARGEBACKS FEED (refunds_raw)
    # -------------------------------------------------------------------------
    n_refunds = max(65, int(n_payments * 0.11))
    refund_sample = payments_df.drop_duplicates(subset=["payment_id"]).sample(
        n=n_refunds, random_state=seed + 13
    )
    refund_ids = [f"REF-50{1000 + i}" for i in range(n_refunds)]
    refund_reasons = rng.choice(
        [
            "DUPLICATE_BILLING",
            "SLA_DOWNTIME_CREDIT",
            "CUSTOMER_CANCELLATION",
            "INVOICE_TAX_ADJUSTMENT",
            "FRAUD_DISPUTE",
        ],
        size=n_refunds,
        p=[0.28, 0.22, 0.26, 0.16, 0.08],
    )
    refund_amounts = np.round(
        np.abs(refund_sample["captured_amount_usd"].values)
        * rng.uniform(0.15, 1.0, size=n_refunds),
        2,
    )
    refund_statuses = rng.choice(
        ["PROCESSED", "PENDING_APPROVAL", "ESCALATED"],
        size=n_refunds,
        p=[0.70, 0.20, 0.10],
    )

    refunds_df = pd.DataFrame(
        {
            "refund_id": refund_ids,
            "payment_id": refund_sample["payment_id"].values,
            "order_id": refund_sample["order_id"].values,
            "refund_amount_usd": refund_amounts,
            "refund_reason": refund_reasons,
            "refund_status": refund_statuses,
            "initiated_timestamp": refund_sample["payment_timestamp"].values,
        }
    )

    # Inject Over-Refund Leakage (refund_amount_usd > captured_amount_usd)
    n_over_ref = max(5, int(n_refunds * cfg["over_refund_rate"]))
    over_idx = rng.choice(refunds_df.index, size=n_over_ref, replace=False)
    base_caps = np.abs(refund_sample.iloc[over_idx]["captured_amount_usd"].values)
    refunds_df.loc[over_idx, "refund_amount_usd"] = np.round(base_caps * 1.45 + 250.0, 2)

    # -------------------------------------------------------------------------
    # 5. ENTERPRISE PIPELINE JOB SCHEDULER TELEMETRY (pipeline_runs)
    # -------------------------------------------------------------------------
    pipelines = [
        ("DAG_SAP_ORDERS_INGEST", "Apache Airflow", "SAP_ERP_CDC", "stg_orders_raw", 180),
        ("DAG_STRIPE_WEBHOOK_ELT", "Apache Airflow", "STRIPE_GLOBAL", "stg_payments_raw", 120),
        ("JOB_KYC_COMPLIANCE_SYNC", "Control-M Enterprise", "KYC_MICROSERVICE", "dim_customer_kyc", 240),
        ("DAG_O2C_RECONCILIATION", "Databricks Workflows", "SILVER_O2C_CORE", "gold_ledger_recon", 300),
        ("JOB_REFUND_AUDIT_FEED", "Control-M Enterprise", "ADYEN_ENTERPRISE", "stg_refunds_raw", 150),
    ]

    run_records = []
    run_counter = 1000
    for day_offset in range(14):
        run_date = base_date + dt.timedelta(days=day_offset)
        for p_name, sched, up_src, tgt_tbl, sla_sec in pipelines:
            for batch_num in (1, 2):
                run_counter += 1
                exec_start = run_date + dt.timedelta(hours=batch_num * 10, minutes=15)
                is_spike = (day_offset in (4, 9, 12, 13)) and (
                    p_name in ("DAG_STRIPE_WEBHOOK_ELT", "DAG_SAP_ORDERS_INGEST", "JOB_KYC_COMPLIANCE_SYNC")
                )
                mult = cfg["sla_breach_boost"] * (1.95 if is_spike else 0.78)
                duration = int(
                    np.clip(rng.normal(loc=sla_sec * mult, scale=35), 35, sla_sec * 3.2)
                )
                rec_in = int(rng.integers(320, 980))
                rec_quar = int(
                    rec_in * (rng.uniform(0.08, 0.19) if is_spike else rng.uniform(0.01, 0.045))
                )

                if duration > sla_sec * 1.35 or rec_quar > rec_in * 0.12:
                    status = "SLA_BREACH_WARNING" if duration <= sla_sec * 1.65 else "FAILED_SCHEMA_CHECK"
                    err_code = (
                        "ERR_PK_DUP_STORM"
                        if "WEBHOOK" in p_name
                        else ("ERR_SCHEMA_NULL_SPIKE" if "ORDERS" in p_name else "ERR_KYC_API_TIMEOUT")
                    )
                elif duration > sla_sec:
                    status = "SLA_BREACH_WARNING"
                    err_code = "WARN_LATENCY_THRESHOLD"
                else:
                    status = "SUCCESS"
                    err_code = "NONE"

                run_records.append(
                    {
                        "dag_run_id": f"RUN-202610-{run_counter}",
                        "pipeline_name": p_name,
                        "scheduler_engine": sched,
                        "upstream_source": up_src,
                        "target_table": tgt_tbl,
                        "execution_start": exec_start.strftime("%Y-%m-%d %H:%M:%S"),
                        "execution_date": exec_start.strftime("%Y-%m-%d"),
                        "duration_sec": duration,
                        "sla_threshold_sec": sla_sec,
                        "sla_breached": duration > sla_sec,
                        "records_ingested": rec_in,
                        "records_quarantined": rec_quar,
                        "job_status": status,
                        "error_code": err_code,
                    }
                )

    pipeline_runs_df = pd.DataFrame(run_records)

    return {
        "customer_kyc_raw": kyc_df,
        "orders_raw": orders_df,
        "payments_raw": payments_df,
        "refunds_raw": refunds_df,
        "pipeline_runs": pipeline_runs_df,
    }
