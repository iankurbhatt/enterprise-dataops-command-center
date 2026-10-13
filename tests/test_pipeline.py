"""
Automated Unit & Integration Test Suite for DataOps Sentinel.
Run with: pytest or python tests/test_pipeline.py
"""

from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.data_generator import SCENARIO_CONFIGS, generate_o2c_datasets
from src.etl_validator import (
    audit_and_cleanse_custom_df,
    profile_dataframe,
    run_o2c_etl_validation,
)
from src.sql_workbench import (
    PREBUILT_SQL_QUERIES,
    build_sqlite_warehouse,
    execute_sql_query,
)
from src.incident_rca import build_incident_backlog, get_incident_rca_deep_dive
from src.genai_copilot import (
    answer_copilot_question,
    generate_executive_stakeholder_brief,
    generate_jira_bug_spec,
    generate_postmortem_report,
)


def test_end_to_end_dataops_pipeline() -> None:
    for scenario_name in SCENARIO_CONFIGS:
        raw = generate_o2c_datasets(n_orders=600, scenario=scenario_name, seed=42)
        assert set(raw.keys()) == {
            "customer_kyc_raw",
            "orders_raw",
            "payments_raw",
            "refunds_raw",
            "pipeline_runs",
        }

        prof = profile_dataframe(raw["orders_raw"])
        assert len(prof) == len(raw["orders_raw"].columns)

        val = run_o2c_etl_validation(raw)
        assert len(val["rule_results"]) == 10
        assert len(val["quarantine_dlq"]) > 0

        # Ensure Silver layer orders have zero duplicate order_ids and zero non-positive amounts
        orders_clean = val["cleansed_datasets"]["orders_clean"]
        assert orders_clean["order_id"].duplicated().sum() == 0
        assert (orders_clean["order_amount_usd"] <= 0).sum() == 0

        # Verify SQLite Warehouse & all 8 prebuilt queries
        conn = build_sqlite_warehouse(raw, val)
        for q_name, q_meta in PREBUILT_SQL_QUERIES.items():
            res_df, err = execute_sql_query(conn, q_meta["sql"])
            assert err is None, f"Query failed: {q_name} -> {err}"
            assert res_df is not None

        # Verify ITSM Incident & 5-Whys RCA generation
        inc_df = build_incident_backlog(val)
        assert len(inc_df) == 7
        rca = get_incident_rca_deep_dive(inc_df.iloc[0]["servicenow_id"], inc_df)
        assert len(rca["five_whys"]) == 5

        # Verify GenAI Copilot documents
        assert "INCIDENT POST-MORTEM" in generate_postmortem_report(rca, scenario_name)
        assert "JIRA ENGINEERING ESCALATION" in generate_jira_bug_spec(rca)
        assert "EXECUTIVE DATA OPERATIONS" in generate_executive_stakeholder_brief(
            val["summary_metrics"], inc_df, scenario_name
        )
        assert len(
            answer_copilot_question(
                "What is the revenue risk?",
                val["summary_metrics"],
                inc_df,
                val["rule_results"],
            )
        ) > 20

    # Verify custom CSV cleanser
    _, clean_custom, stats = audit_and_cleanse_custom_df(raw["orders_raw"].head(120))
    assert stats["clean_rows"] <= stats["raw_rows"]
    assert clean_custom.isna().sum().sum() == 0


if __name__ == "__main__":
    test_end_to_end_dataops_pipeline()
    print("All end-to-end DataOps Sentinel tests passed!")
