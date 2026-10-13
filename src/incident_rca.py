"""
ServiceNow & JIRA Incident Management + Systematic Root Cause Analysis (RCA) Engine.
Automatically translates ETL validation failures and scheduler SLA breaches into
prioritized ITSM incidents with 5-Whys diagnostics and lineage tracing.
"""

from __future__ import annotations

from typing import Any, Dict
import pandas as pd


INCIDENT_TEMPLATES = {
    "RULE-01": {
        "snow_id": "INC0084910",
        "jira_key": "DOPS-1401",
        "title": "Duplicate Order PK Ingestion Spike in SAP/Stripe Batch Feeds",
        "pipeline": "DAG_SAP_ORDERS_INGEST",
        "scheduler": "Apache Airflow",
        "upstream_system": "STRIPE_CHECKOUT / API_GATEWAY",
        "downstream_impact": "Executive GMV Dashboard & FinOps Order Ledger",
        "assignment_group": "DataOps Pipeline Engineering",
        "mttr_mins": 34,
        "sla_target_mins": 60,
        "state": "Auto-Remediated (Verified)",
        "root_cause_category": "Idempotency / Webhook Retry Storm",
        "five_whys": [
            "Why 1: Duplicate order_id rows appeared in stg_orders_raw during batch BATCH-20261013-RETRY.",
            "Why 2: The ingestion API received multiple identical POST payloads within a 90-second window.",
            "Why 3: Upstream payment checkout gateway experienced a transient HTTP 504 timeout and triggered exponential webhook retries.",
            "Why 4: The Bronze-to-Silver ELT staging loader lacked a deterministic ROW_NUMBER() OVER (PARTITION BY order_id) deduplication gate.",
            "Why 5 (Root Cause): Ingestion pipeline was configured for append-only load instead of idempotent MERGE/UPSERT on primary key.",
        ],
        "immediate_remediation": "Executed automated Pandas/SQL deduplication retaining earliest ingestion timestamp and quarantined duplicate retry payloads.",
        "preventive_action": "Deployed idempotent MERGE INTO statement in Silver transformation DAG and enforced unique key constraint in schema registry.",
        "patch_snippet": """-- Idempotent Silver Layer Merge Fix (DOPS-1401)
DELETE FROM stg_orders_raw
WHERE rowid NOT IN (
    SELECT MIN(rowid)
    FROM stg_orders_raw
    GROUP BY order_id
);""",
    },
    "RULE-06": {
        "snow_id": "INC0084914",
        "jira_key": "DOPS-1405",
        "title": "Orphan Payment Captures Missing Parent Order FK (CDC Replication Lag)",
        "pipeline": "DAG_STRIPE_WEBHOOK_ELT",
        "scheduler": "Apache Airflow",
        "upstream_system": "STRIPE_GLOBAL / SAP_ERP_CDC",
        "downstream_impact": "Daily Cash-to-Order Reconciliation Report",
        "assignment_group": "DataOps L3 & Integration Team",
        "mttr_mins": 48,
        "sla_target_mins": 60,
        "state": "In Progress - RCA",
        "root_cause_category": "Asynchronous CDC Replication Skew",
        "five_whys": [
            "Why 1: Payment capture records in payments_raw failed foreign key validation against orders_raw.order_id.",
            "Why 2: Payment events arrived in the warehouse 18 minutes before the corresponding ERP order records.",
            "Why 3: Stripe webhook streaming ingestion runs every 2 minutes, while SAP_ERP_CDC order sync runs on a 15-minute micro-batch.",
            "Why 4: Downstream reconciliation DAG executed immediately after payment load without checking upstream order watermark.",
            "Why 5 (Root Cause): Missing cross-DAG ExternalTaskSensor dependency and absence of a late-arriving dimension holding queue.",
        ],
        "immediate_remediation": "Routed unlinked payment captures to CDC Holding Queue (quarantine_dlq) and triggered ad-hoc order sync backfill.",
        "preventive_action": "Added Airflow ExternalTaskSensor watermark alignment and a 20-minute retry window for orphan foreign keys.",
        "patch_snippet": """# Airflow Watermark Sensor Alignment (DOPS-1405)
wait_for_orders_cdc = ExternalTaskSensor(
    task_id="wait_for_sap_orders_cdc",
    external_dag_id="DAG_SAP_ORDERS_INGEST",
    external_task_id="validate_orders_silver",
    execution_delta=timedelta(minutes=15),
    mode="reschedule",
    timeout=1200,
)""",
    },
    "RULE-09": {
        "snow_id": "INC0084918",
        "jira_key": "DOPS-1409",
        "title": "Unverified/Expired KYC Accounts Exceeding High-Value Transaction Limits",
        "pipeline": "JOB_KYC_COMPLIANCE_SYNC",
        "scheduler": "Control-M Enterprise",
        "upstream_system": "KYC_MICROSERVICE",
        "downstream_impact": "Regulatory AML Audit Feed & Treasury Settlement",
        "assignment_group": "Compliance & Risk DataOps",
        "mttr_mins": 42,
        "sla_target_mins": 45,
        "state": "Escalated to Compliance & Eng",
        "root_cause_category": "Stale Reference Data & Constraint Bypass",
        "five_whys": [
            "Why 1: High-value orders (>$15,000) were processed for customers marked UNVERIFIED or EXPIRED in customer_kyc_raw.",
            "Why 2: Order checkout service validated against a cached KYC status table that was 36 hours out of sync.",
            "Why 3: Control-M batch job JOB_KYC_COMPLIANCE_SYNC timed out during nightly full-refresh due to connection pool exhaustion.",
            "Why 4: Checkout API fallback policy defaulted to 'ALLOW_WITH_ASYNC_AUDIT' when KYC cache TTL expired.",
            "Why 5 (Root Cause): Lack of hard pre-settlement constraint gate in ETL pipeline for transactions exceeding tier thresholds.",
        ],
        "immediate_remediation": "Placed automated COMPLIANCE_HOLD flag on all flagged high-value orders and isolated records in quarantine_dlq.",
        "preventive_action": "Implemented strict pre-settlement constraint validation gate (RULE-09) and switched KYC sync to incremental CDC.",
        "patch_snippet": """-- Pre-Settlement Compliance Hold Gate (DOPS-1409)
UPDATE stg_orders_raw
SET order_status = 'ON_HOLD_KYC_COMPLIANCE'
WHERE customer_id IN (
    SELECT customer_id FROM dim_customer_kyc
    WHERE kyc_verification_status IN ('UNVERIFIED', 'EXPIRED')
       OR aml_risk_score > 85.0
) AND net_order_total_usd > 5000.00;""",
    },
    "RULE-02": {
        "snow_id": "INC0084922",
        "jira_key": "DOPS-1412",
        "title": "Null Spike in Mandatory customer_id and merchant_region Attributes",
        "pipeline": "DAG_SAP_ORDERS_INGEST",
        "scheduler": "Apache Airflow",
        "upstream_system": "SALESFORCE_CPQ / SHOPIFY_PLUS_API",
        "downstream_impact": "Regional Revenue Attribution & Territory KPI Deck",
        "assignment_group": "DataOps Quality Assurance",
        "mttr_mins": 29,
        "sla_target_mins": 90,
        "state": "Auto-Remediated (Verified)",
        "root_cause_category": "Upstream Payload Schema Drift",
        "five_whys": [
            "Why 1: Completeness check RULE-02 flagged NULL values in customer_id and merchant_region.",
            "Why 2: JSON payload parser extracted NULL for `merchant_region` on newly onboarded APAC/LATAM orders.",
            "Why 3: Upstream Salesforce CPQ release v4.8 renamed `merchant_region` to `territory_code` without updating data contract.",
            "Why 4: Ingestion parser used permissive JSON key lookup `.get('merchant_region')` without fallback alias mapping.",
            "Why 5 (Root Cause): Missing automated JSON schema contract validation in CI/CD before upstream producer deployment.",
        ],
        "immediate_remediation": "Imputed merchant_region from customer country lookup where possible and quarantined missing customer_id rows.",
        "preventive_action": "Updated ingestion mapper to coalesce (`merchant_region`, `territory_code`) and added JSON Schema contract enforcement.",
        "patch_snippet": """# Schema Drift Resilient Field Mapper (DOPS-1412)
df["merchant_region"] = (
    df["merchant_region"]
    .fillna(df.get("territory_code"))
    .fillna(df["country_code"].map(COUNTRY_TO_REGION_MAP))
    .fillna("UNASSIGNED_REGION")
)""",
    },
    "RULE-03": {
        "snow_id": "INC0084926",
        "jira_key": "DOPS-1416",
        "title": "Negative Order Amounts & Inverted Promotional Discount Anomalies",
        "pipeline": "DAG_SAP_ORDERS_INGEST",
        "scheduler": "Apache Airflow",
        "upstream_system": "SHOPIFY_PLUS_API",
        "downstream_impact": "Net Revenue KPI & Tax Reporting Feed",
        "assignment_group": "DataOps Pipeline Engineering",
        "mttr_mins": 25,
        "sla_target_mins": 60,
        "state": "Auto-Remediated (Verified)",
        "root_cause_category": "Credit Memo Misclassification & Promo Stacking Bug",
        "five_whys": [
            "Why 1: Orders with negative `order_amount_usd` or `discount_usd > order_amount_usd` entered `orders_raw`.",
            "Why 2: Credit memos and stacked promotional vouchers were emitted to the standard order topic instead of the adjustments feed.",
            "Why 3: Upstream e-commerce promo engine allowed stacking two 70% discount codes on bundle renewals.",
            "Why 4: Raw ingestion table accepted unconstrained FLOAT values without CHECK constraints.",
            "Why 5 (Root Cause): Absence of domain range validation (`order_amount_usd > 0` and `discount_usd <= order_amount_usd`) at Bronze gate.",
        ],
        "immediate_remediation": "Quarantined negative/inverted records in DLQ and excluded from Silver production aggregation.",
        "preventive_action": "Added strict Pandas/SQL range validation gate and separated credit memo routing in upstream event bus.",
        "patch_snippet": """-- Domain Constraint Enforcement (DOPS-1416)
SELECT * FROM stg_orders_raw
WHERE order_amount_usd > 0
  AND discount_usd BETWEEN 0 AND order_amount_usd
  AND net_order_total_usd > 0;""",
    },
    "RULE-07": {
        "snow_id": "INC0084930",
        "jira_key": "DOPS-1420",
        "title": "Order-to-Payment Settlement Variance Exceeding Tolerance ($0.05)",
        "pipeline": "DAG_O2C_RECONCILIATION",
        "scheduler": "Databricks Workflows",
        "upstream_system": "ADYEN_ENTERPRISE / STRIPE_GLOBAL",
        "downstream_impact": "Month-End Financial Close & GL Reconciliation",
        "assignment_group": "FinOps & Data Operations",
        "mttr_mins": 52,
        "sla_target_mins": 120,
        "state": "In Progress - FinOps Review",
        "root_cause_category": "FX Conversion Rounding & Partial Capture Drift",
        "five_whys": [
            "Why 1: Captured payment amounts differed from net order totals by more than $0.05 across multi-currency orders.",
            "Why 2: Order checkout locked FX rate at cart creation, whereas payment gateway applied spot FX rate at capture time.",
            "Why 3: Split shipments triggered partial payment captures without updating the parent order invoice record.",
            "Why 4: Reconciliation pipeline compared raw `captured_amount_usd` directly against `net_order_total_usd` without FX tolerance bands.",
            "Why 5 (Root Cause): Missing FX settlement timestamp join and partial-capture aggregation in Silver reconciliation model.",
        ],
        "immediate_remediation": "Flagged all variance records in `quarantine_dlq` and generated automated FinOps reconciliation variance ledger.",
        "preventive_action": "Enhanced reconciliation model to aggregate partial captures by `order_id` and join daily Treasury FX rate table.",
        "patch_snippet": """-- Multi-Capture Aggregation & FX Reconciliation Fix (DOPS-1420)
WITH AggPayments AS (
    SELECT order_id, SUM(captured_amount_usd) AS total_captured_usd
    FROM payments_clean
    GROUP BY order_id
)
SELECT o.order_id, o.net_order_total_usd, p.total_captured_usd
FROM orders_clean o
JOIN AggPayments p ON o.order_id = p.order_id
WHERE ABS(o.net_order_total_usd - p.total_captured_usd) > 0.05;""",
    },
    "RULE-10": {
        "snow_id": "INC0084935",
        "jira_key": "DOPS-1424",
        "title": "Over-Refund Revenue Leakage Exceeding Original Captured Payment",
        "pipeline": "JOB_REFUND_AUDIT_FEED",
        "scheduler": "Control-M Enterprise",
        "upstream_system": "ADYEN_ENTERPRISE",
        "downstream_impact": "Revenue Assurance & Chargeback Loss Reserve",
        "assignment_group": "Revenue Assurance DataOps",
        "mttr_mins": 38,
        "sla_target_mins": 60,
        "state": "Escalated to FinOps",
        "root_cause_category": "Duplicate Credit Issuance / Uncapped Refund API",
        "five_whys": [
            "Why 1: Refund records in `refunds_raw` exceeded 100% of the original `captured_amount_usd`.",
            "Why 2: Support agents issued manual SLA credit adjustments on top of automated full-order refunds.",
            "Why 3: Refund API endpoint did not validate cumulative historical refunds against original payment capture.",
            "Why 4: Batch refund ingestion job loaded records without cross-checking `payments_raw.captured_amount_usd`.",
            "Why 5 (Root Cause): Missing cross-table constraint validation between `refunds_raw` and `payments_raw` prior to payout batch.",
        ],
        "immediate_remediation": "Capped refund disbursement at 100% of captured payment in Silver table and quarantined excess leakage rows.",
        "preventive_action": "Implemented real-time cumulative refund ceiling check in payout pipeline.",
        "patch_snippet": """-- Cumulative Refund Ceiling Constraint (DOPS-1424)
SELECT r.refund_id, r.payment_id, r.refund_amount_usd, p.captured_amount_usd
FROM stg_refunds_raw r
JOIN stg_payments_raw p ON r.payment_id = p.payment_id
WHERE r.refund_amount_usd > ABS(p.captured_amount_usd);""",
    },
}


def build_incident_backlog(validation_output: Dict[str, Any]) -> pd.DataFrame:
    """
    Build a dynamic ServiceNow & JIRA incident table populated with live counts
    and financial exposure from the current ETL validation run.
    """
    rule_df = validation_output["rule_results"].set_index("rule_id")
    incidents = []

    for rule_id, meta in INCIDENT_TEMPLATES.items():
        if rule_id not in rule_df.index:
            continue
        r_row = rule_df.loc[rule_id]
        failed_cnt = int(r_row["records_failed"])
        rev_risk = float(r_row["revenue_at_risk_usd"])
        sev = str(r_row["severity"])

        if failed_cnt > 50 or rev_risk > 150000:
            priority = "P1 - Critical"
        elif sev == "CRITICAL" or failed_cnt > 20:
            priority = "P1 - Critical" if rev_risk > 60000 else "P2 - High"
        else:
            priority = "P2 - High" if failed_cnt > 10 else "P3 - Moderate"

        sla_status = (
            "WITHIN SLA"
            if meta["mttr_mins"] <= meta["sla_target_mins"] * 0.85
            else ("AT RISK" if meta["mttr_mins"] <= meta["sla_target_mins"] else "BREACHED")
        )

        incidents.append(
            {
                "servicenow_id": meta["snow_id"],
                "jira_ticket": meta["jira_key"],
                "rule_id": rule_id,
                "priority": priority,
                "incident_title": meta["title"],
                "affected_pipeline": meta["pipeline"],
                "scheduler_engine": meta["scheduler"],
                "upstream_source": meta["upstream_system"],
                "assignment_group": meta["assignment_group"],
                "ticket_state": meta["state"],
                "impacted_records": failed_cnt,
                "revenue_at_risk_usd": round(rev_risk, 2),
                "mttr_mins": meta["mttr_mins"],
                "sla_target_mins": meta["sla_target_mins"],
                "sla_status": sla_status,
                "root_cause_category": meta["root_cause_category"],
            }
        )

    df = pd.DataFrame(incidents)
    return df.sort_values(by=["priority", "revenue_at_risk_usd"], ascending=[True, False]).reset_index(
        drop=True
    )


def get_incident_rca_deep_dive(
    servicenow_id: str, incidents_df: pd.DataFrame
) -> Dict[str, Any]:
    """
    Retrieve the full 5-Whys Root Cause Analysis, lineage chain, and remediation
    patch for a selected ServiceNow incident.
    """
    row = incidents_df[incidents_df["servicenow_id"] == servicenow_id].iloc[0]
    rule_id = row["rule_id"]
    template = INCIDENT_TEMPLATES[rule_id]

    return {
        "incident": row.to_dict(),
        "downstream_impact": template["downstream_impact"],
        "five_whys": template["five_whys"],
        "immediate_remediation": template["immediate_remediation"],
        "preventive_action": template["preventive_action"],
        "patch_snippet": template["patch_snippet"],
    }
