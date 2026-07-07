-- init/01_create_tables.sql
-- Runs once on first container start (when the volume is empty)

CREATE TABLE IF NOT EXISTS pipeline_runs (
    id          SERIAL PRIMARY KEY,
    pipeline    TEXT        NOT NULL,
    status      TEXT        NOT NULL,   -- success / failed
    rows_loaded INT,
    started_at  TIMESTAMP   DEFAULT NOW(),
    finished_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS data_quality_log (
    id          SERIAL PRIMARY KEY,
    table_name  TEXT        NOT NULL,
    check_name  TEXT        NOT NULL,
    passed      BOOLEAN     NOT NULL,
    details     TEXT,
    checked_at  TIMESTAMP   DEFAULT NOW()
);