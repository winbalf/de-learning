#!/bin/bash

# ============================================================
# run_pipeline.sh
# Week 1 DE pipeline runner — downloads data, runs ETL, logs output
# ============================================================

set -e  # exit immediately if any command fails

# --- Config ---
LOG_FILE="pipeline_$(date +%Y%m%d_%H%M%S).log"
DATA_DIR="$HOME/de-learning/week1"
VENV="$HOME/de-learning/.venv"

log() {
  echo "$(date '+%Y-%m-%d %H:%M:%S') $1" | tee -a "$LOG_FILE"
}

# --- Start ---
log "INFO  Pipeline started"
log "INFO  Log file: $LOG_FILE"

# --- Activate virtual environment ---
log "INFO  Activating virtual environment"
source "$VENV/bin/activate"

# --- Check data file exists, download if missing ---
cd "$DATA_DIR"

if [ -f "titanic.csv" ]; then
  log "INFO  titanic.csv already exists — skipping download"
else
  log "INFO  Downloading titanic.csv"
  curl -s -O https://raw.githubusercontent.com/datasciencedojo/datasets/master/titanic.csv
  log "INFO  Download complete"
fi

# --- Row count check ---
ROW_COUNT=$(tail -n +2 titanic.csv | wc -l)
log "INFO  titanic.csv has $ROW_COUNT data rows"

if [ "$ROW_COUNT" -lt 100 ]; then
  log "ERROR File has fewer than 100 rows — aborting"
  exit 1
fi

# --- Run ETL script ---
log "INFO  Running ETL pipeline"

if python db_pipeline.py >> "$LOG_FILE" 2>&1; then
  log "INFO  ETL complete"
else
  log "ERROR ETL script failed — check log for details"
  exit 1
fi

# --- Summary ---
log "INFO  Pipeline finished successfully"
echo ""
echo "=== Error summary ==="
grep "ERROR" "$LOG_FILE" || echo "No errors found"

echo ""
echo "=== Log saved to: $LOG_FILE ==="