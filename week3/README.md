# Week 3 — Docker Compose, Named Volumes, Dockerfile & Pipeline Observability

**Theme:** Replace bare `docker run` commands with a full Compose stack. Persist data across restarts. Containerise the Python pipeline. Track every run with pipeline observability tables.

> **Prerequisites:** [Week 1](../week1/README.md) and [Week 2](../week2/README.md) complete · Docker Desktop running on WSL2

---

## Table of Contents
1. [Folder structure](#1-folder-structure)
2. [Docker Compose — single file, full stack](#2-docker-compose--single-file-full-stack)
3. [Named volumes — data that survives restarts](#3-named-volumes--data-that-survives-restarts)
4. [Init SQL — tables created automatically on first boot](#4-init-sql--tables-created-automatically-on-first-boot)
5. [pgAdmin — visual database browser](#5-pgadmin--visual-database-browser)
6. [Dockerfile — containerise your Python pipeline](#6-dockerfile--containerise-your-python-pipeline)
7. [Environment variables — no hardcoded credentials](#7-environment-variables--no-hardcoded-credentials)
8. [Pipeline observability](#8-pipeline-observability)
9. [Common commands](#9-common-commands)
10. [Key concepts](#10-key-concepts)
11. [Week 3 checklist](#11-week-3-checklist)

---

## 1. Folder structure

```
week3/
├── docker-compose.yml        # full stack definition
├── Dockerfile                # pipeline container image
├── requirements.txt          # Python dependencies for the container
├── pipeline_runner.py        # ETL + DQ + observability
├── db_utils.py               # copied from week2 — self-contained
├── logs/                     # pipeline logs (mounted from container)
└── init/
    └── 01_create_tables.sql  # runs automatically on first Postgres boot
```

---

## 2. Docker Compose — single file, full stack

`docker-compose.yml`:

```yaml
services:

  postgres:
    image: postgres:15
    container_name: de-postgres
    environment:
      POSTGRES_USER: deuser
      POSTGRES_PASSWORD: depassword
      POSTGRES_DB: delearning
    ports:
      - "5432:5432"
    volumes:
      - postgres_data:/var/lib/postgresql/data
      - ./init:/docker-entrypoint-initdb.d
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U deuser -d delearning"]
      interval: 5s
      timeout: 5s
      retries: 5
    restart: unless-stopped

  pgadmin:
    image: dpage/pgadmin4:latest
    container_name: de-pgadmin
    environment:
      PGADMIN_DEFAULT_EMAIL: admin@gmail.com
      PGADMIN_DEFAULT_PASSWORD: admin
    ports:
      - "8080:80"
    volumes:
      - pgadmin_data:/var/lib/pgadmin
    depends_on:
      postgres:
        condition: service_healthy
    restart: unless-stopped

  pipeline:
    build:
      context: .
      dockerfile: Dockerfile
    container_name: de-pipeline
    environment:
      DB_HOST: postgres
      DB_PORT: 5432
      DB_NAME: delearning
      DB_USER: deuser
      DB_PASSWORD: depassword
    volumes:
      - ./logs:/app/logs
    depends_on:
      postgres:
        condition: service_healthy
    restart: "no"

volumes:
  postgres_data:
  pgadmin_data:
```

**Key points:**

| Setting | Why it matters |
|---|---|
| `depends_on: condition: service_healthy` | Pipeline waits for Postgres to be ready — no race condition |
| `DB_HOST: postgres` | Docker resolves service names as hostnames on the internal network |
| `restart: unless-stopped` | Postgres and pgAdmin auto-restart on crash or machine reboot |
| `restart: "no"` | Pipeline runs once and exits cleanly — it's not a daemon |
| No `version:` key | Removed — obsolete in modern Docker Compose, causes warnings |

**Essential commands:**

```bash
docker compose up -d                    # start all services in background
docker compose up -d postgres pgadmin   # start specific services only
docker compose run --rm pipeline        # run pipeline once, remove container after
docker compose down                     # stop and remove containers (volumes preserved)
docker compose down -v                  # stop AND delete volumes (data gone)
docker compose ps                       # check status of all services
docker compose logs --tail=20 pipeline  # tail logs of a service
docker compose build pipeline           # rebuild image after code changes
```

---

## 3. Named volumes — data that survives restarts

**The week 2 problem:** bare `docker run` containers lose all data when removed. The named volume fixes this.

```bash
# Prove data persists
docker exec -it de-postgres psql -U deuser -d delearning -c \
  "INSERT INTO pipeline_runs (pipeline, status) VALUES ('test', 'success');"

docker compose down          # destroy containers
docker compose up -d         # recreate them
sleep 3

# Data is still there
docker exec -it de-postgres psql -U deuser -d delearning -c \
  "SELECT * FROM pipeline_runs;"
```

**How it works:**

```
docker-compose.yml declares:  volumes: postgres_data:
                                              ↓
Docker creates a volume at:   /var/lib/docker/volumes/week3_postgres_data/
                                              ↓
Container mounts it at:       /var/lib/postgresql/data
                                              ↓
Container deleted → volume stays → new container mounts same volume → data intact
```

Inspect volumes:
```bash
docker volume ls
docker volume inspect week3_postgres_data
```

---

## 4. Init SQL — tables created automatically on first boot

Any `.sql` file placed in `./init/` runs once when Postgres first starts on an empty volume.

`init/01_create_tables.sql`:

```sql
CREATE TABLE IF NOT EXISTS pipeline_runs (
    id          SERIAL PRIMARY KEY,
    pipeline    TEXT        NOT NULL,
    status      TEXT        NOT NULL,
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
```

Verify tables were created:
```bash
docker exec -it de-postgres psql -U deuser -d delearning -c "\dt"
```

> **Note:** Init scripts only run on first boot (empty volume). To re-run them, delete the volume first: `docker compose down -v`

---

## 5. pgAdmin — visual database browser

pgAdmin is added as a second service in `docker-compose.yml`.

**Access:** `http://<WSL-IP>:8080`

Get your WSL IP:
```bash
ip addr show eth0 | grep "inet " | awk '{print $2}' | cut -d/ -f1
```

Or open directly:
```bash
wslview http://$(ip addr show eth0 | grep "inet " | awk '{print $2}' | cut -d/ -f1):8080
```

**Login:** `admin@gmail.com` / `admin`

> **Note:** pgAdmin now rejects `.local` email domains. Use a standard domain like `@gmail.com`.

**Connect to Postgres inside pgAdmin:**
1. Click **Add New Server**
2. General tab → Name: `de-local`
3. Connection tab:
   - Host: `postgres` (Docker service name — not localhost)
   - Port: `5432`
   - Database: `delearning`
   - Username: `deuser`
   - Password: `depassword`

**WSL2 port forwarding** (if `localhost:8080` doesn't work in Windows browser):

Run in PowerShell as Administrator:
```powershell
$wslIP = (wsl hostname -I).Trim().Split()[0]
netsh interface portproxy add v4tov4 listenport=8080 listenaddress=0.0.0.0 connectport=8080 connectaddress=$wslIP
netsh interface portproxy show all
```

---

## 6. Dockerfile — containerise your Python pipeline

`Dockerfile`:

```dockerfile
FROM python:3.11-slim

LABEL maintainer="de-learning"

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["python", "pipeline_runner.py"]
```

`requirements.txt`:

```
pandas==2.2.2
sqlalchemy==2.0.30
psycopg2-binary==2.9.9
pyarrow==16.0.0
```

**Why each Dockerfile instruction matters:**

| Instruction | Why |
|---|---|
| `FROM python:3.11-slim` | Slim base — ~130MB vs ~900MB for full image |
| `PYTHONDONTWRITEBYTECODE=1` | No `.pyc` files — keeps image clean |
| `PYTHONUNBUFFERED=1` | Logs appear immediately — essential for debugging |
| `COPY requirements.txt` first | Docker layer caching — if deps unchanged, pip is skipped on rebuild |
| `COPY . .` after pip | Code changes don't invalidate the pip cache layer |
| `CMD` not `ENTRYPOINT` | CMD can be overridden at runtime — more flexible |

Build and run:
```bash
docker compose build pipeline
docker compose run --rm pipeline
```

---

## 7. Environment variables — no hardcoded credentials

`pipeline_runner.py` reads all DB credentials from environment variables with sensible local defaults:

```python
engine = get_engine(
    host=os.getenv("DB_HOST", "localhost"),
    port=int(os.getenv("DB_PORT", 5432)),
    db=os.getenv("DB_NAME", "delearning"),
    user=os.getenv("DB_USER", "deuser"),
    password=os.getenv("DB_PASSWORD", "depassword"),
)
```

This means:
- **Locally** (plain Python): uses `localhost` defaults — works as before
- **Inside Docker**: `DB_HOST=postgres` from `docker-compose.yml` — resolves via Docker network
- **In production** (AWS/GCP): env vars injected by the platform — no code changes needed

> **Rule:** Never hardcode credentials in code. Always use environment variables. Secrets go in `.env` files (gitignored) or a secrets manager in production.

---

## 8. Pipeline observability

Every pipeline run is tracked in `pipeline_runs`. Every DQ check is logged in `data_quality_log`. This is the foundation of pipeline monitoring.

**`log_pipeline_start`** — inserts a `running` row, returns the ID:
```python
run_id = log_pipeline_start(engine, "titanic_summary_refresh")
```

**`log_pipeline_end`** — updates status, row count, and finish time:
```python
log_pipeline_end(engine, run_id, status="success", rows_loaded=3)
```

**`run_data_quality_checks`** — runs 4 checks and logs each result:
```python
all_passed = run_data_quality_checks(engine, "titanic")
```

DQ checks implemented:

| Check | What it verifies |
|---|---|
| `no_null_names` | No missing passenger names |
| `no_negative_fares` | No negative ticket prices |
| `valid_ages` | All ages between 0 and 120 |
| `unique_passengers` | No duplicate passenger IDs |

Query pipeline history:
```bash
docker exec -it de-postgres psql -U deuser -d delearning -c \
  "SELECT id, pipeline, status, rows_loaded, started_at, finished_at FROM pipeline_runs ORDER BY started_at DESC LIMIT 10;"
```

Query DQ log:
```bash
docker exec -it de-postgres psql -U deuser -d delearning -c \
  "SELECT * FROM data_quality_log ORDER BY checked_at DESC LIMIT 10;"
```

**What the audit trail looks like:**

```
 id  pipeline                  status    rows_loaded
  7  titanic_summary_refresh   success   3
  6  titanic_summary_refresh   success   3
  5  titanic_summary_refresh   success   3
  3  titanic_summary_refresh   failed    NULL   ← column name error
```

Failed runs show `NULL` rows and preserve the error context — you can see exactly when it broke and when it recovered.

---

## 9. Common commands

```bash
# Start full stack
docker compose up -d

# Run pipeline once
docker compose run --rm pipeline

# Rebuild pipeline image after code changes
docker compose build pipeline && docker compose run --rm pipeline

# View logs
docker compose logs --tail=50 pipeline
docker compose logs --tail=20 postgres

# Check all service health
docker compose ps

# Connect to Postgres directly
docker exec -it de-postgres psql -U deuser -d delearning

# Stop stack (keep volumes)
docker compose down

# Stop stack AND delete all data
docker compose down -v

# List volumes
docker volume ls

# Inspect a volume
docker volume inspect week3_postgres_data
```

---

## 10. Key concepts

| Concept | Why it matters |
|---|---|
| `docker compose up -d` | One command starts your entire stack — reproducible everywhere |
| Named volumes | Data survives container removal — essential for stateful services |
| `depends_on: service_healthy` | Prevents race conditions — pipeline waits for DB to be ready |
| Docker internal network | Service names resolve as hostnames — `postgres` not `localhost` |
| Init SQL scripts | Schema bootstrapped automatically — no manual setup |
| `FROM python:3.11-slim` | Slim images — faster builds, smaller attack surface |
| Layer caching | `COPY requirements.txt` before code — pip only reruns when deps change |
| Environment variables | Credentials injected at runtime — same image, any environment |
| `restart: unless-stopped` | Services recover from crashes automatically |
| `restart: "no"` | One-shot containers exit cleanly — right for ETL pipelines |
| Pipeline observability | Every run tracked — know immediately when something broke |

---

## 11. Week 3 checklist

- [x] Docker Compose stack with Postgres + pgAdmin + pipeline
- [x] Named volumes — data persists across container restarts
- [x] Proved volume persistence with destroy + recreate test
- [x] Init SQL — `pipeline_runs` and `data_quality_log` auto-created on boot
- [x] pgAdmin connected and browsing tables visually
- [x] WSL2 port forwarding for Windows browser access
- [x] Dockerfile — pipeline containerised with slim Python base
- [x] Layer caching — requirements copied before code
- [x] Environment variables — no hardcoded credentials
- [x] `pipeline_runner.py` — ETL + DQ checks + observability in one script
- [x] `docker compose run --rm pipeline` — full containerised run
- [x] Git commit

---

**Next:** [Week 4 — Apache Airflow · DAGs · scheduling · operators](../week4/README.md)