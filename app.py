"""
DataOps Sentinel — Enterprise ETL Observability, Data Quality & Incident RCA Command Center
Portfolio Project by Ankur Bhatt (Senior Analyst — Data Operations & Analytics)
"""

from __future__ import annotations

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

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


# -----------------------------------------------------------------------------
# PAGE CONFIGURATION & CUSTOM STYLING
# -----------------------------------------------------------------------------
st.set_page_config(
    page_title="DataOps Sentinel | Ankur Bhatt Portfolio",
    page_icon="🛡️",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown(
    """
    <style>
    .main-header {
        background: linear-gradient(135deg, #0F172A 0%, #1E293B 60%, #1D4ED8 100%);
        padding: 1.4rem 1.8rem;
        border-radius: 12px;
        border: 1px solid rgba(59, 130, 246, 0.35);
        margin-bottom: 1.2rem;
    }
    .main-header h1 {
        color: #F8FAFC;
        font-size: 1.75rem;
        margin: 0 0 0.35rem 0;
        font-weight: 700;
    }
    .main-header p {
        color: #CBD5E1;
        font-size: 0.96rem;
        margin: 0;
    }
    .skill-badge {
        display: inline-block;
        background: rgba(59, 130, 246, 0.18);
        color: #93C5FD;
        border: 1px solid rgba(59, 130, 246, 0.4);
        border-radius: 6px;
        padding: 2px 8px;
        font-size: 0.76rem;
        font-weight: 600;
        margin: 2px 3px 2px 0;
    }
    .lineage-box {
        background: #111827;
        border: 1px solid #374151;
        border-left: 4px solid #3B82F6;
        padding: 0.85rem 1.1rem;
        border-radius: 8px;
        margin-bottom: 0.8rem;
    }
    .why-card {
        background: rgba(30, 41, 59, 0.75);
        border-left: 3px solid #F59E0B;
        padding: 0.65rem 1rem;
        border-radius: 6px;
        margin-bottom: 0.5rem;
        font-size: 0.92rem;
    }
    </style>
    """,
    unsafe_allow_html=True,
)


# -----------------------------------------------------------------------------
# CACHED PIPELINE EXECUTION
# -----------------------------------------------------------------------------
@st.cache_data(show_spinner=False)
def load_and_validate_pipeline(n_orders: int, scenario: str, seed: int):
    raw_datasets = generate_o2c_datasets(n_orders=n_orders, scenario=scenario, seed=seed)
    validation_output = run_o2c_etl_validation(raw_datasets)
    incidents_df = build_incident_backlog(validation_output)
    return raw_datasets, validation_output, incidents_df


# -----------------------------------------------------------------------------
# SIDEBAR: AUTHOR PROFILE & SIMULATION CONTROLS
# -----------------------------------------------------------------------------
with st.sidebar:
    st.markdown("### 🛡️ DataOps Sentinel v2.4")
    st.caption("Enterprise ETL Observability, Data Cleansing & ITSM RCA Platform")
    st.divider()

    st.markdown("#### ⚙️ Live Pipeline Simulator")
    selected_scenario = st.selectbox(
        "Operational Feed Scenario",
        options=list(SCENARIO_CONFIGS.keys()),
        index=0,
        help="Switch scenarios to simulate production incidents (Webhook retry storms, ERP schema drift, or KYC compliance outages).",
    )
    st.info(SCENARIO_CONFIGS[selected_scenario]["description"])

    batch_size = st.slider(
        "Order Ingestion Volume (Rows)",
        min_value=500,
        max_value=3000,
        value=1200,
        step=250,
    )
    sim_seed = st.number_input("Simulation Batch Seed", min_value=1, max_value=9999, value=42)

    st.divider()
    st.markdown("#### 👨‍💻 Built By: Ankur Bhatt")
    st.caption("Senior Analyst — Data Operations & Analytics (4+ Yrs IT | TCS)")
    st.markdown(
        """
        <div>
            <span class="skill-badge">Python</span>
            <span class="skill-badge">Pandas</span>
            <span class="skill-badge">NumPy</span>
            <span class="skill-badge">SQL</span>
            <span class="skill-badge">ETL / ELT</span>
            <span class="skill-badge">Data Cleansing</span>
            <span class="skill-badge">Schema Validation</span>
            <span class="skill-badge">ServiceNow</span>
            <span class="skill-badge">JIRA</span>
            <span class="skill-badge">Root Cause Analysis</span>
            <span class="skill-badge">Power BI KPIs</span>
            <span class="skill-badge">GenAI Automation</span>
        </div>
        """,
        unsafe_allow_html=True,
    )
    st.markdown(
        "🔗 [LinkedIn Profile](https://www.linkedin.com/in/iankurbhatt) | "
        "📧 [iankurbhatt1@gmail.com](mailto:iankurbhatt1@gmail.com)"
    )


# -----------------------------------------------------------------------------
# LOAD DATA & INITIALIZE WAREHOUSE
# -----------------------------------------------------------------------------
raw_datasets, validation_output, incidents_df = load_and_validate_pipeline(
    n_orders=batch_size, scenario=selected_scenario, seed=int(sim_seed)
)
sqlite_conn = build_sqlite_warehouse(raw_datasets, validation_output)
metrics = validation_output["summary_metrics"]
rule_results_df = validation_output["rule_results"]
quarantine_df = validation_output["quarantine_dlq"]


# -----------------------------------------------------------------------------
# HERO BANNER & TOP EXECUTIVE KPI SCORECARD
# -----------------------------------------------------------------------------
st.markdown(
    f"""
    <div class="main-header">
        <h1>🛡️ DataOps Sentinel — Enterprise Pipeline & Data Quality Command Center</h1>
        <p>End-to-End Order-to-Cash (O2C) ETL Ingestion, Automated Constraint Validation, SQL Reconciliation & ServiceNow/JIRA Root Cause Analysis | <b>Active Scenario:</b> {selected_scenario}</p>
    </div>
    """,
    unsafe_allow_html=True,
)

kpi1, kpi2, kpi3, kpi4, kpi5, kpi6 = st.columns(6)
p1_incidents = int((incidents_df["priority"] == "P1 - Critical").sum())
avg_mttr = round(float(incidents_df["mttr_mins"].mean()), 1)

kpi1.metric(
    "Data Quality Score",
    f"{metrics['overall_health_score']:.2f}%",
    delta="99.94% Silver Layer",
)
kpi2.metric(
    "Total Constraint Checks",
    f"{metrics['total_evaluated_checks']:,}",
    delta=f"10 Active Rules",
)
kpi3.metric(
    "Quarantined Records (DLQ)",
    f"{metrics['total_quarantined_rows']:,}",
    delta=f"-{metrics['total_failed_checks']:,} Anomalies Isolated",
    delta_color="inverse",
)
kpi4.metric(
    "Revenue Protected (USD)",
    f"${metrics['total_revenue_at_risk_usd']:,.0f}",
    delta="100% Intercepted Pre-GL",
)
kpi5.metric(
    "Active ITSM Incidents",
    f"{len(incidents_df)} Tickets",
    delta=f"{p1_incidents} P1-Critical",
    delta_color="inverse" if p1_incidents > 2 else "normal",
)
kpi6.metric(
    "Mean Resolution (MTTR)",
    f"{avg_mttr} mins",
    delta="100% Within SLA",
)


# -----------------------------------------------------------------------------
# MAIN NAVIGATION TABS
# -----------------------------------------------------------------------------
tab_exec, tab_etl, tab_sql, tab_rca, tab_ai, tab_client = st.tabs(
    [
        "📊 1. Executive KPI Command Center",
        "⚙️ 2. ETL Profiling & Validation Engine",
        "🗄️ 3. SQL Reconciliation Workbench",
        "🚨 4. ServiceNow / JIRA & 5-Whys RCA",
        "🤖 5. GenAI Ops Copilot & Runbooks",
        "📤 6. Client Custom CSV Audit Tool",
    ]
)


# =============================================================================
# TAB 1: EXECUTIVE KPI & PIPELINE COMMAND CENTER
# =============================================================================
with tab_exec:
    st.subheader("📈 Enterprise Pipeline Observability & Commercial Impact Scorecard")
    col_left, col_right = st.columns(2)

    with col_left:
        fig_rules = px.bar(
            rule_results_df.sort_values("revenue_at_risk_usd", ascending=True),
            x="revenue_at_risk_usd",
            y="rule_name",
            color="severity",
            orientation="h",
            title="Commercial Revenue at Risk Intercepted by Validation Rule ($ USD)",
            labels={
                "revenue_at_risk_usd": "Revenue Exposure ($ USD)",
                "rule_name": "Validation Rule",
                "severity": "Severity",
            },
            color_discrete_map={
                "CRITICAL": "#EF4444",
                "HIGH": "#F59E0B",
                "MEDIUM": "#3B82F6",
            },
            template="plotly_dark",
        )
        fig_rules.update_layout(height=390, margin=dict(l=10, r=10, t=45, b=10))
        st.plotly_chart(fig_rules, use_container_width=True)

    with col_right:
        runs_df = raw_datasets["pipeline_runs"]
        daily_runs = (
            runs_df.groupby(["execution_date", "pipeline_name"], as_index=False)[
                ["duration_sec", "sla_threshold_sec", "records_quarantined"]
            ]
            .mean()
        )
        fig_sla = px.line(
            daily_runs,
            x="execution_date",
            y="duration_sec",
            color="pipeline_name",
            markers=True,
            title="14-Day Enterprise Job Scheduler Execution Duration (Sec) vs. Spikes",
            labels={
                "execution_date": "Execution Date",
                "duration_sec": "Avg Duration (Seconds)",
                "pipeline_name": "Pipeline / DAG",
            },
            template="plotly_dark",
        )
        fig_sla.update_layout(height=390, margin=dict(l=10, r=10, t=45, b=10))
        st.plotly_chart(fig_sla, use_container_width=True)

    col_b1, col_b2 = st.columns(2)
    with col_b1:
        orders_clean = validation_output["cleansed_datasets"]["orders_clean"]
        reg_summary = (
            orders_clean.groupby(["merchant_region", "product_category"], as_index=False)[
                "net_order_total_usd"
            ]
            .sum()
        )
        fig_reg = px.bar(
            reg_summary,
            x="merchant_region",
            y="net_order_total_usd",
            color="product_category",
            title="Verified Silver-Layer Net Order Volume by Region & Product Category ($ USD)",
            labels={
                "merchant_region": "Merchant Region",
                "net_order_total_usd": "Verified Net Order Total ($ USD)",
                "product_category": "Product Suite",
            },
            template="plotly_dark",
        )
        fig_reg.update_layout(height=370, margin=dict(l=10, r=10, t=45, b=10))
        st.plotly_chart(fig_reg, use_container_width=True)

    with col_b2:
        dim_summary = (
            rule_results_df.groupby("dimension", as_index=False)[
                ["records_failed", "revenue_at_risk_usd"]
            ]
            .sum()
        )
        fig_dim = px.pie(
            dim_summary,
            names="dimension",
            values="records_failed",
            hole=0.48,
            title="Quarantined Anomalies Distribution by Data Quality Dimension",
            template="plotly_dark",
        )
        fig_dim.update_layout(height=370, margin=dict(l=10, r=10, t=45, b=10))
        st.plotly_chart(fig_dim, use_container_width=True)

    st.markdown("#### 🕒 Enterprise Scheduler Telemetry Summary (Airflow, Control-M, Databricks)")
    sched_summary = (
        runs_df.groupby(["pipeline_name", "scheduler_engine", "upstream_source", "target_table"], as_index=False)
        .agg(
            total_runs=("dag_run_id", "count"),
            sla_breaches=("sla_breached", "sum"),
            avg_duration_sec=("duration_sec", "mean"),
            sla_limit_sec=("sla_threshold_sec", "max"),
            total_ingested=("records_ingested", "sum"),
            total_quarantined=("records_quarantined", "sum"),
        )
    )
    sched_summary["avg_duration_sec"] = sched_summary["avg_duration_sec"].round(1)
    sched_summary["sla_compliance_pct"] = (
        (1 - sched_summary["sla_breaches"] / sched_summary["total_runs"]) * 100
    ).round(1)
    st.dataframe(sched_summary, use_container_width=True, hide_index=True)


# =============================================================================
# TAB 2: ETL PROFILING, VALIDATION & QUARANTINE ENGINE
# =============================================================================
with tab_etl:
    st.subheader("⚙️ Automated ETL Schema & Business Constraint Validation Gate")
    st.caption(
        "Evaluates 10 production Data Quality rules across Completeness, Uniqueness, Validity, Referential Integrity, Accuracy, Timeliness, and Compliance."
    )

    st.dataframe(
        rule_results_df,
        use_container_width=True,
        hide_index=True,
        column_config={
            "pass_rate_pct": st.column_config.ProgressColumn(
                "Pass Rate (%)", min_value=0, max_value=100, format="%.2f%%"
            ),
            "revenue_at_risk_usd": st.column_config.NumberColumn(
                "Revenue at Risk ($ USD)", format="$%.2f"
            ),
        },
    )

    st.divider()
    col_prof1, col_prof2 = st.columns([1, 3])
    with col_prof1:
        st.markdown("#### 🔍 Column-Level Data Profiler")
        feed_choice = st.selectbox(
            "Select Feed to Profile",
            options=[
                "orders_raw (Bronze Staging)",
                "orders_clean (Silver Validated)",
                "payments_raw (Bronze Staging)",
                "payments_clean (Silver Validated)",
                "customer_kyc_raw (Bronze Staging)",
                "refunds_raw (Bronze Staging)",
            ],
        )
        feed_key = feed_choice.split(" ")[0]
        target_df = (
            raw_datasets[feed_key]
            if feed_key in raw_datasets
            else validation_output["cleansed_datasets"][feed_key]
        )
        st.metric("Row Count", f"{len(target_df):,}")
        st.metric("Column Count", f"{len(target_df.columns)}")
        st.metric("Duplicate Rows", f"{int(target_df.duplicated().sum()):,}")

    with col_prof2:
        st.markdown(f"#### 📋 Automated Column Profile: `{feed_key}`")
        prof_df = profile_dataframe(target_df)
        st.dataframe(prof_df, use_container_width=True, hide_index=True)

    st.divider()
    st.markdown("#### 🚧 Dead-Letter Queue (DLQ) — Quarantined Records Inspector")
    dlq_col1, dlq_col2, dlq_col3 = st.columns(3)
    with dlq_col1:
        sel_rules = st.multiselect(
            "Filter by Rule ID",
            options=sorted(quarantine_df["rule_id"].unique()),
            default=sorted(quarantine_df["rule_id"].unique()),
        )
    with dlq_col2:
        sel_sev = st.multiselect(
            "Filter by Severity",
            options=sorted(quarantine_df["severity"].unique()),
            default=sorted(quarantine_df["severity"].unique()),
        )
    with dlq_col3:
        st.write("")
        st.download_button(
            label="📥 Download Quarantine DLQ (CSV)",
            data=quarantine_df.to_csv(index=False).encode("utf-8"),
            file_name="dataops_quarantine_dlq.csv",
            mime="text/csv",
            use_container_width=True,
        )

    filtered_dlq = quarantine_df[
        quarantine_df["rule_id"].isin(sel_rules) & quarantine_df["severity"].isin(sel_sev)
    ]
    st.dataframe(filtered_dlq, use_container_width=True, hide_index=True, height=320)


# =============================================================================
# TAB 3: INTERACTIVE SQL RECONCILIATION WORKBENCH
# =============================================================================
with tab_sql:
    st.subheader("🗄️ Interactive SQL Reconciliation & Audit Workbench (SQLite Warehouse)")
    st.caption(
        "Run pre-built operational reconciliation queries or write custom SQL against Bronze (`orders_raw`, `payments_raw`, `customer_kyc_raw`, `refunds_raw`) and Silver (`orders_clean`, `payments_clean`, `quarantine_dlq`) tables."
    )

    q_choice = st.selectbox(
        "Select Pre-Built Operational Audit Query",
        options=list(PREBUILT_SQL_QUERIES.keys()),
    )
    q_meta = PREBUILT_SQL_QUERIES[q_choice]
    st.info(f"**Category:** `{q_meta['category']}` — {q_meta['description']}")

    custom_sql = st.text_area(
        "SQL Query Editor (Editable — Supports CTEs, Window Functions, Joins & Aggregations)",
        value=q_meta["sql"],
        height=220,
    )

    run_col1, run_col2 = st.columns([1, 4])
    with run_col1:
        exec_btn = st.button("▶️ Execute SQL Query", type="primary", use_container_width=True)

    if exec_btn or custom_sql:
        res_df, err_msg = execute_sql_query(sqlite_conn, custom_sql)
        if err_msg:
            st.error(f"SQL Execution Error: {err_msg}")
        elif res_df is not None:
            st.success(f"Query executed successfully — returned **{len(res_df):,} rows**.")
            st.dataframe(res_df, use_container_width=True, hide_index=True)
            st.download_button(
                "📥 Export Query Results (CSV)",
                data=res_df.to_csv(index=False).encode("utf-8"),
                file_name="sql_reconciliation_output.csv",
                mime="text/csv",
            )


# =============================================================================
# TAB 4: SERVICENOW / JIRA INCIDENT TRACKER & 5-WHYS RCA
# =============================================================================
with tab_rca:
    st.subheader("🚨 ServiceNow & JIRA Incident Management + Systematic 5-Whys Root Cause Analysis")
    st.caption(
        "Translates pipeline anomalies into prioritized ITSM incidents with SLA tracking, upstream lineage mapping, and 5-Whys engineering diagnostics."
    )

    st.dataframe(
        incidents_df,
        use_container_width=True,
        hide_index=True,
        column_config={
            "revenue_at_risk_usd": st.column_config.NumberColumn(
                "Financial Exposure ($ USD)", format="$%.2f"
            ),
        },
    )

    st.divider()
    st.markdown("#### 🔬 Deep-Dive Systematic Root Cause Analysis (RCA) Inspector")
    inc_options = [
        f"{r['servicenow_id']} | {r['jira_ticket']} | {r['priority']} — {r['incident_title']}"
        for _, r in incidents_df.iterrows()
    ]
    selected_inc_label = st.selectbox("Select Incident for 5-Whys Diagnostic Walkthrough", options=inc_options)
    selected_snow_id = selected_inc_label.split(" | ")[0]
    rca_bundle = get_incident_rca_deep_dive(selected_snow_id, incidents_df)
    inc_info = rca_bundle["incident"]

    st.markdown(
        f"""
        <div class="lineage-box">
            <b>🔗 End-to-End Operational Lineage Impact Chain:</b><br/>
            <code>Upstream Source: {inc_info['upstream_source']}</code> ➔
            <code>Scheduler: {inc_info['scheduler_engine']} ({inc_info['affected_pipeline']})</code> ➔
            <code>Validation Gate: {inc_info['rule_id']} ({inc_info['impacted_records']} records quarantined)</code> ➔
            <code>Downstream Consumer Protected: {rca_bundle['downstream_impact']}</code>
        </div>
        """,
        unsafe_allow_html=True,
    )

    rca_c1, rca_c2 = st.columns([3, 2])
    with rca_c1:
        st.markdown("##### 🧭 Systematic 5-Whys Diagnostic Chain")
        for why_step in rca_bundle["five_whys"]:
            st.markdown(f'<div class="why-card">{why_step}</div>', unsafe_allow_html=True)

    with rca_c2:
        st.markdown("##### 🛠️ Containment & Preventive Engineering Action")
        st.success(f"**Immediate DataOps Remediation:**\n\n{rca_bundle['immediate_remediation']}")
        st.warning(f"**Long-Term Preventive Fix ({inc_info['jira_ticket']}):**\n\n{rca_bundle['preventive_action']}")
        st.markdown("**Production Remediation Code / SQL Patch:**")
        st.code(rca_bundle["patch_snippet"], language="sql")


# =============================================================================
# TAB 5: GENAI OPS COPILOT & AUTOMATED RUNBOOK GENERATOR
# =============================================================================
with tab_ai:
    st.subheader("🤖 GenAI DataOps Copilot & Automated Executive Documentation")
    st.caption(
        "Automates incident post-mortems, JIRA engineering handoff specs, and executive commercial risk briefs from live pipeline telemetry."
    )

    doc_col1, doc_col2 = st.columns([1, 2])
    with doc_col1:
        st.markdown("#### 📄 Automated Artifact Generator")
        target_inc_for_doc = st.selectbox(
            "Select Incident Ticket",
            options=incidents_df["servicenow_id"].tolist(),
            format_func=lambda x: f"{x} ({incidents_df[incidents_df['servicenow_id']==x]['jira_ticket'].iloc[0]})",
        )
        doc_type = st.radio(
            "Select Document Template",
            options=[
                "1. ServiceNow / Confluence Incident Post-Mortem (RCA)",
                "2. JIRA Engineering Defect & Patch Specification",
                "3. Executive Commercial & Risk Stakeholder Brief",
            ],
        )

        st.divider()
        st.markdown("#### 💬 Ask the DataOps AI Assistant")
        preset_q = st.selectbox(
            "Quick Stakeholder Prompts",
            options=[
                "What is our total commercial revenue at risk and how is it protected?",
                "Summarize all active P1 critical ServiceNow/JIRA incidents and MTTR.",
                "What are the root causes of the webhook duplicate and orphan payment anomalies?",
                "Give me an overall pipeline health readiness summary for leadership.",
            ],
        )
        user_q = st.text_input("Or ask a custom question about current pipeline operations:", value=preset_q)
        if user_q:
            st.markdown(
                answer_copilot_question(user_q, metrics, incidents_df, rule_results_df)
            )

    with doc_col2:
        selected_rca_bundle = get_incident_rca_deep_dive(target_inc_for_doc, incidents_df)
        if doc_type.startswith("1."):
            generated_md = generate_postmortem_report(selected_rca_bundle, selected_scenario)
            fname = f"PostMortem_{target_inc_for_doc}.md"
        elif doc_type.startswith("2."):
            generated_md = generate_jira_bug_spec(selected_rca_bundle)
            fname = f"JIRA_Spec_{selected_rca_bundle['incident']['jira_ticket']}.md"
        else:
            generated_md = generate_executive_stakeholder_brief(
                metrics, incidents_df, selected_scenario
            )
            fname = "Executive_DataOps_Stakeholder_Brief.md"

        st.download_button(
            f"📥 Download {fname}",
            data=generated_md.encode("utf-8"),
            file_name=fname,
            mime="text/markdown",
            type="primary",
        )
        with st.container(border=True):
            st.markdown(generated_md)


# =============================================================================
# TAB 6: CLIENT CUSTOM CSV AUDIT & CLEANSING SANDBOX
# =============================================================================
with tab_client:
    st.subheader("📤 Client Custom Dataset Profiler & Automated Cleansing Sandbox")
    st.caption(
        "Built for live client consultations & interviews: upload any raw CSV dataset to automatically profile column quality, strip duplicates, impute missing values, flag extreme IQR outliers, and export a clean Silver dataset."
    )

    uploaded_file = st.file_uploader(
        "Upload a Raw CSV File for Instant DataOps Audit & Cleansing (or leave empty to test with sample messy feed)",
        type=["csv"],
    )

    if uploaded_file is not None:
        client_raw_df = pd.read_csv(uploaded_file)
        source_label = uploaded_file.name
    else:
        client_raw_df = raw_datasets["orders_raw"].head(300).copy()
        source_label = "Sample_Messy_Orders_Feed.csv (Built-in Demo)"

    prof_before, cleansed_client_df, client_stats = audit_and_cleanse_custom_df(client_raw_df)

    st.markdown(f"##### 📊 Automated Cleansing Results for `{source_label}`")
    c_m1, c_m2, c_m3, c_m4, c_m5 = st.columns(5)
    c_m1.metric("Raw Input Rows", f"{client_stats['raw_rows']:,}")
    c_m2.metric("Clean Output Rows", f"{client_stats['clean_rows']:,}")
    c_m3.metric("Exact Duplicates Removed", f"{client_stats['duplicates_removed']:,}")
    c_m4.metric("Null Cells Remediated", f"{client_stats['null_cells_remediated']:,}")
    c_m5.metric(
        "Data Health Score",
        f"{client_stats['data_quality_score_after']}%",
        delta=f"+{client_stats['data_quality_score_after'] - client_stats['data_quality_score_before']:.1f}% Improvement",
    )

    col_cl1, col_cl2 = st.columns(2)
    with col_cl1:
        st.markdown("**Column-Level Quality Profile (Before Cleansing)**")
        st.dataframe(prof_before, use_container_width=True, hide_index=True, height=300)
    with col_cl2:
        st.markdown("**Cleansed & Standardized Output Preview (After Cleansing)**")
        st.dataframe(cleansed_client_df.head(100), use_container_width=True, hide_index=True, height=300)

    st.download_button(
        label="📥 Download Cleansed Dataset (CSV)",
        data=cleansed_client_df.to_csv(index=False).encode("utf-8"),
        file_name=f"cleansed_{source_label}",
        mime="text/csv",
        type="primary",
    )
