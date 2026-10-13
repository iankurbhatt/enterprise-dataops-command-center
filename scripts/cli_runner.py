"""
Headless CLI Runner for Enterprise Job Schedulers (Apache Airflow / Control-M / Cron).
Executes the Order-to-Cash ETL validation suite, prints structured telemetry,
and exports the Quarantine Dead-Letter Queue (DLQ) and ServiceNow incident log.
"""

from __future__ import annotations

import argparse
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.data_generator import generate_o2c_datasets
from src.etl_validator import run_o2c_etl_validation
from src.incident_rca import build_incident_backlog


def main() -> int:
    parser = argparse.ArgumentParser(
        description="DataOps Sentinel Headless Batch ETL & Validation Runner"
    )
    parser.add_argument(
        "--orders",
        type=int,
        default=1200,
        help="Number of raw order records to ingest (default: 1200)",
    )
    parser.add_argument(
        "--scenario",
        type=str,
        default="Standard Production Operations (Baseline)",
        help="Operational scenario preset",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default="data/exports",
        help="Directory to export quarantine DLQ and incident CSV artifacts",
    )
    args = parser.parse_args()

    print("=" * 78)
    print("[DATAOPS SENTINEL] Starting Batch ETL & Constraint Validation Pipeline")
    print(f"Scenario : {args.scenario}")
    print(f"Batch Vol: {args.orders} orders")
    print("=" * 78)

    raw_data = generate_o2c_datasets(n_orders=args.orders, scenario=args.scenario)
    val_out = run_o2c_etl_validation(raw_data)
    incidents = build_incident_backlog(val_out)
    metrics = val_out["summary_metrics"]

    os.makedirs(args.output_dir, exist_ok=True)
    dlq_path = os.path.join(args.output_dir, "quarantine_dlq.csv")
    inc_path = os.path.join(args.output_dir, "servicenow_incidents.csv")
    rules_path = os.path.join(args.output_dir, "validation_rule_results.csv")

    val_out["quarantine_dlq"].to_csv(dlq_path, index=False)
    incidents.to_csv(inc_path, index=False)
    val_out["rule_results"].to_csv(rules_path, index=False)

    print(f"[METRIC] Overall Data Quality Health Score : {metrics['overall_health_score']:.2f}%")
    print(f"[METRIC] Total Constraint Checks Evaluated : {metrics['total_evaluated_checks']:,}")
    print(f"[METRIC] Total Quarantined Records (DLQ)   : {metrics['total_quarantined_rows']:,}")
    print(f"[METRIC] Commercial Revenue Protected (USD): ${metrics['total_revenue_at_risk_usd']:,.2f}")
    print(f"[METRIC] Active ServiceNow/JIRA Incidents  : {len(incidents)}")
    print("-" * 78)
    print(f"[EXPORT] Quarantine DLQ saved to           : {dlq_path}")
    print(f"[EXPORT] ServiceNow Incident Backlog saved : {inc_path}")
    print("[STATUS] Batch ETL & Validation Completed Successfully (Exit Code 0)")
    print("=" * 78)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
