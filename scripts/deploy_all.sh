#!/bin/bash
# ==============================================================
# Full deployment script for E-Commerce Customer Support Chatbot
# Runs all phases: DAB deploy → Pipeline job → Metric Views → Genie Space
#
# Usage:
#   ./scripts/deploy_all.sh <databricks-cli-profile>
#
# Example:
#   ./scripts/deploy_all.sh fe-vm-vdm-serverless-iwnbow
# ==============================================================

set -euo pipefail

PROFILE="${1:?Usage: ./scripts/deploy_all.sh <databricks-cli-profile>}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

echo "=============================================="
echo " E-Commerce Chatbot - Full Deployment"
echo " Profile: $PROFILE"
echo "=============================================="

# --------------------------------------------------
# Step 1: Validate & Deploy DAB (Bronze + Silver + Gold pipelines + Job)
# --------------------------------------------------
echo ""
echo "[1/4] Deploying Databricks Asset Bundle..."
cd "$PROJECT_DIR"
databricks bundle validate -t dev
databricks bundle deploy -t dev --auto-approve
echo "      DAB deployed."

# --------------------------------------------------
# Step 2: Run the pipeline orchestration job (Bronze → Silver → Gold)
# --------------------------------------------------
echo ""
echo "[2/4] Running pipeline job (Bronze → Silver → Gold)..."
databricks bundle run ecom_pipeline_job -t dev
echo "      Pipeline job completed."

# --------------------------------------------------
# Step 3: Create Metric Views (Semantic Layer)
# --------------------------------------------------
echo ""
echo "[3/4] Creating UC Metric Views..."
WAREHOUSE_ID=$(databricks api get /api/2.0/sql/warehouses --profile "$PROFILE" --output json \
  | python3 -c "import sys,json; ws=json.load(sys.stdin).get('warehouses',[]); r=[w for w in ws if w.get('state')=='RUNNING']; print((r or ws)[0]['id'] if ws else '')")

if [ -z "$WAREHOUSE_ID" ]; then
  echo "ERROR: No SQL warehouse found."
  exit 1
fi
echo "      Using warehouse: $WAREHOUSE_ID"

# Read and execute each CREATE VIEW statement
python3 -c "
import subprocess, sys

sql_file = '$PROJECT_DIR/src/metrics/create_metric_views.sql'
with open(sql_file) as f:
    content = f.read()

# Split on semicolons to get individual statements
statements = [s.strip() for s in content.split(';') if s.strip() and s.strip().startswith('CREATE')]

for i, stmt in enumerate(statements):
    print(f'      Creating metric view {i+1}/{len(statements)}...')
    result = subprocess.run(
        ['databricks', 'api', 'post', '/api/2.0/sql/statements',
         '--profile', '$PROFILE', '--json',
         '{\"warehouse_id\": \"$WAREHOUSE_ID\", \"statement\": ' + repr(stmt) + ', \"wait_timeout\": \"30s\"}'],
        capture_output=True, text=True
    )
    if result.returncode != 0:
        print(f'      WARN: {result.stderr.strip()}', file=sys.stderr)
    else:
        print(f'      Done.')
"
echo "      Metric views created."

# --------------------------------------------------
# Step 4: Create Genie Space
# --------------------------------------------------
echo ""
echo "[4/4] Creating Genie Space..."
python3 "$PROJECT_DIR/src/genie/create_genie_space.py" --profile "$PROFILE" --replace
echo "      Genie Space created."

# --------------------------------------------------
# Done
# --------------------------------------------------
echo ""
echo "=============================================="
echo " Deployment complete!"
echo "=============================================="
echo ""
echo " Pipelines:    databricks bundle run ecom_pipeline_job -t dev"
echo " Genie Space:  see src/genie/genie_space_config.json for URL"
echo " Destroy:      databricks bundle destroy -t dev"
echo ""
