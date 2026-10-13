#!/usr/bin/env bash
# =============================================================================
# DataOps Sentinel — Enterprise Batch ETL & Constraint Validation Wrapper
# Author: Ankur Bhatt (Senior Analyst — Data Operations & Analytics)
# Scheduler Compatibility: Apache Airflow BashOperator / Control-M / Cron
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
TIMESTAMP="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
LOG_DIR="${PROJECT_ROOT}/data/exports"
mkdir -p "$LOG_DIR"

echo "[$TIMESTAMP] [INFO] Initializing DataOps Sentinel Batch Pipeline..."
cd "$PROJECT_ROOT"

 ORDERS_COUNT="${1:-1200}"
 SCENARIO_NAME="${2:-Standard Production Operations (Baseline)}"

python scripts/cli_runner.py \
  --orders "$ORDERS_COUNT" \
  --scenario "$SCENARIO_NAME" \
  --output-dir "$LOG_DIR"

echo "[$TIMESTAMP] [SUCCESS] Pipeline run completed. Artifacts written to ${LOG_DIR}"
