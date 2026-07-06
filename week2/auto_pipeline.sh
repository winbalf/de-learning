#!/bin/bash
# ============================================================
# auto_pipeline.sh
# Simulates a scheduled DE pipeline — runs a summary refresh
# and logs results with rotation
# ============================================================

set -e

# --- Config ---
BASE_DIR="$HOME/de-learning"
WEEK2_DIR="$BASE_DIR/week2"
VENV="$BASE_DIR/.venv"
LOG_DIR="$WEEK2_DIR/logs"
MAX_LOGS=5   # keep only last 5 log files

mkdir -p "$LOG_DIR"

TIMESTAMP=$(date +%Y%m%d_%H%M%S)
LOG_FILE="$LOG_DIR/pipeline_${TIMESTAMP}.log"

log() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') [$1] $2" | tee -a "$LOG_FILE"
}

# --- Log rotation: delete oldest logs beyond MAX_LOGS ---
rotate_logs() {
    local count
    count=$(ls "$LOG_DIR"/pipeline_*.log 2>/dev/null | wc -l)
    if [ "$count" -gt "$MAX_LOGS" ]; then
        ls -t "$LOG_DIR"/pipeline_*.log | tail -n +$((MAX_LOGS + 1)) | xargs rm -f
        log "INFO" "Log rotation: kept $MAX_LOGS most recent logs"
    fi
}

# --- Check postgres is reachable ---
check_db() {
    docker exec de-postgres pg_isready -U deuser -d delearning > /dev/null 2>&1
}

# --- Main ---
log "INFO" "Pipeline started"
log "INFO" "Log: $LOG_FILE"

# Activate venv
source "$VENV/bin/activate"
log "INFO" "Virtual environment activated"

# DB health check
log "INFO" "Checking database connection..."
if check_db; then
    log "INFO" "Database is ready"
else
    log "ERROR" "Database not reachable — aborting"
    exit 1
fi

# Run the summary refresh
log "INFO" "Running summary refresh..."
cd "$WEEK2_DIR"

python - << 'PYEOF'
import sys
sys.path.insert(0, '.')
from db_utils import get_engine, run_query, load_dataframe

engine = get_engine()

summary = run_query(engine, """
    SELECT
        "Pclass" as pclass,
        COUNT(*)                            AS total,
        SUM("Survived")                     AS survived,
        ROUND(AVG("Fare")::numeric, 2)      AS avg_fare,
        ROUND(100.0 * SUM("Survived") /
              COUNT(*)::numeric, 1)         AS survival_pct,
        ROUND(AVG("Age")::numeric, 1)       AS avg_age
    FROM titanic
    GROUP BY "Pclass"
    ORDER BY "Pclass"
""")

print(summary.to_string(index=False))
load_dataframe(engine, summary, "titanic_summary", if_exists="replace")
engine.dispose()
PYEOF

log "INFO" "Summary refresh complete"

# Report
log "INFO" "Checking log directory..."
LOG_COUNT=$(ls "$LOG_DIR"/pipeline_*.log 2>/dev/null | wc -l)
log "INFO" "Total log files: $LOG_COUNT"
rotate_logs

log "INFO" "Pipeline finished successfully"
echo ""
echo "=== Latest log: $LOG_FILE ==="