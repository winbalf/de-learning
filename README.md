# Data Engineering Study Plan

**Month 1 — Foundations**

> **Setup:** WSL2 Ubuntu on Windows · Python 3.11 · 5–10 hrs/week target

This repo is a hands-on data engineering curriculum. Each week lives in its own folder with a dedicated README — start here, then follow the week-by-week guides.

---

## Quick start

```bash
cd ~/de-learning
source .venv/bin/activate   # create in Week 1 if you haven't yet
```

New to the project? Begin with [Week 1](week1/README.md) to set up your environment and build your first pipeline.

---

## Curriculum

| Week | Topic | Guide |
|---|---|---|
| **1** | Environment, pandas, Parquet, PostgreSQL, shell | [week1/README.md](week1/README.md) |
| **2** | Advanced SQL, DB patterns, automation | [week2/README.md](week2/README.md) |
| **3** | Docker Compose, named volumes, Dockerfile, observability | [week3/README.md](week3/README.md) |
| **4** | Apache Airflow — DAGs, operators, sensors, XComs | [week4/README.md](week4/README.md) |
| **5** | dbt — models, tests, documentation | [week5/README.md](week5/README.md) |
| **6** | PySpark — DataFrames, Spark SQL, medallion architecture | [week6/README.md](week6/README.md) |

---

## What you'll build

```
Week 1   CSV → pandas → Parquet → PostgreSQL          (first ETL pipeline)
Week 2   Window functions · query optimisation · cron  (SQL depth + automation)
Week 3   Docker Compose stack · containerised pipeline (production-like infra)
Week 4   Airflow DAGs orchestrating the titanic flow   (industry-standard scheduler)
Week 5   dbt staging → intermediate → marts + tests    (SQL transformations)
Week 6   PySpark bronze → silver → gold                (lakehouse pattern at scale)
```

All weeks use the **Titanic dataset** as a consistent thread — each week adds a new layer on top of the previous one.

---

## Project structure

```
de-learning/
├── .venv/              # shared Python virtual environment
├── week1/              # pandas, Parquet, PostgreSQL, bash
├── week2/              # SQL, db_utils, cron automation
├── week3/              # Docker Compose, Dockerfile, observability
├── week4/              # Airflow DAGs and operators
├── week5/              # dbt models, tests, docs
├── week6/              # PySpark DataFrames, SQL, medallion
└── README.md           # this file — navigation hub
```

---

## Progress

- [x] Week 1 — Environment & first pipeline
- [x] Week 2 — Advanced SQL & automation
- [x] Week 3 — Docker Compose & observability
- [x] Week 4 — Apache Airflow
- [x] Week 5 — dbt · models · tests · documentation
- [x] Week 6 — PySpark · DataFrames · Spark SQL · medallion architecture
- [ ] Week 7 — Portfolio project · end-to-end pipeline · GitHub · interview prep

---

**Start here:** [Week 1 — Environment, pandas, Parquet, PostgreSQL & Shell](week1/README.md)
