# Week 5 — dbt: Models, Tests & Documentation

**Theme:** Transform raw data into clean analytical tables using SQL. Every transformation version-controlled, tested, and documented automatically.

> **Prerequisites:** [Week 3](../week3/README.md) complete · `de-postgres` running on port 5433 · titanic table loaded

---

## Table of Contents
1. [What dbt is and where it fits](#1-what-dbt-is-and-where-it-fits)
2. [Installation](#2-installation)
3. [Project structure](#3-project-structure)
4. [Connection profile](#4-connection-profile)
5. [Three-layer architecture](#5-three-layer-architecture)
6. [Sources](#6-sources)
7. [Staging models](#7-staging-models)
8. [Intermediate models](#8-intermediate-models)
9. [Mart models](#9-mart-models)
10. [Generic tests](#10-generic-tests)
11. [Singular tests](#11-singular-tests)
12. [Documentation](#12-documentation)
13. [Essential commands](#13-essential-commands)
14. [Key concepts](#14-key-concepts)
15. [Week 5 checklist](#15-week-5-checklist)

---

## 1. What dbt is and where it fits

dbt handles the **T** in ELT — it transforms data already in your warehouse using SQL. It does not extract or load.

```
Python pipeline (weeks 1-3)    → Extract + Load  → raw tables in Postgres
dbt (week 5)                   → Transform       → clean analytical tables
```

| Without dbt | With dbt |
|---|---|
| SQL scripts scattered in folders | Version-controlled models |
| No data quality checks | 26 tests, all automated |
| No documentation | Auto-generated docs + lineage graph |
| Changes break things silently | `dbt test` catches regressions |
| One giant transformation | Layered models — easy to debug |

---

## 2. Installation

```bash
cd ~/de-learning
source .venv/bin/activate

# Install stable version — dbt 2.x alpha breaks Postgres support
pip install dbt-core==1.8.2 dbt-postgres==1.8.2

dbt --version
# Should show: dbt-core 1.8.2, dbt-postgres 1.8.2
```

> **Note:** `dbt-postgres` only goes up to `1.8.2`. If `dbt init` triggers a 2.x alpha version, uninstall and reinstall `dbt-core==1.8.2 dbt-postgres==1.8.2` explicitly.

---

## 3. Project structure

```
week5/titanic_dbt/
├── dbt_project.yml               # project config
├── models/
│   ├── staging/
│   │   ├── sources.yml           # define raw source tables
│   │   ├── stg_titanic.sql       # clean + rename columns
│   │   └── stg_titanic.yml       # column descriptions + generic tests
│   ├── intermediate/
│   │   ├── int_passengers_enriched.sql   # derived fields, segments
│   │   └── int_passengers_enriched.yml
│   └── marts/
│       ├── fct_survival_by_class.sql
│       ├── fct_survival_by_class.yml
│       ├── fct_survival_by_demographics.sql
│       └── fct_survival_by_demographics.yml
└── tests/
    ├── test_survival_rate_bounds.sql          # singular test
    └── test_total_passengers_matches_source.sql
```

`dbt_project.yml`:
```yaml
name: 'titanic_dbt'
version: '1.0.0'
config-version: 2

profile: 'titanic_dbt'

model-paths: ["models"]
test-paths: ["tests"]
macro-paths: ["macros"]
seed-paths: ["seeds"]

target-path: "target"
clean-targets: ["target", "dbt_packages"]

models:
  titanic_dbt:
    staging:
      +materialized: view      # views — always fresh, zero storage cost
    intermediate:
      +materialized: view
    marts:
      +materialized: table     # tables — fast to query for analysts
```

---

## 4. Connection profile

`~/.dbt/profiles.yml`:
```yaml
titanic_dbt:
  target: dev
  outputs:
    dev:
      type: postgres
      host: localhost
      port: 5433          # de-postgres maps to 5433 on the host
      user: deuser
      password: depassword
      dbname: delearning
      schema: dbt_dev     # dbt creates this schema — keeps raw data separate
      threads: 4
```

Test connection:
```bash
cd ~/de-learning/week5/titanic_dbt
dbt debug
# All checks passed → Connection test: OK
```

---

## 5. Three-layer architecture

```
sources (raw tables in public schema)
        ↓
staging models (views)      — clean, rename, cast, handle nulls
        ↓
intermediate models (views) — join, enrich, business logic
        ↓
marts models (tables)       — final analytical tables for analysts
```

**Why three layers:**
- Each layer has one job — easy to debug when something breaks
- Staging broke → source data changed
- Mart broke → business logic changed
- Analysts always query marts, never raw tables

---

## 6. Sources

Define raw tables so dbt can track lineage and run freshness checks:

`models/staging/sources.yml`:
```yaml
version: 2

sources:
  - name: raw
    database: delearning
    schema: public
    description: "Raw data loaded by the Python ETL pipeline"

    tables:
      - name: titanic
        description: "Raw Titanic passenger data — 891 rows"
        columns:
          - name: PassengerId
            description: "Unique passenger identifier"
          - name: Survived
            description: "0 = died, 1 = survived"
          - name: Pclass
            description: "Ticket class: 1 = first, 2 = second, 3 = third"
          - name: Fare
            description: "Ticket price in British pounds"
          - name: Embarked
            description: "Port: C=Cherbourg, Q=Queenstown, S=Southampton"
```

Reference in SQL: `{{ source('raw', 'titanic') }}`

---

## 7. Staging models

One model per source table. Jobs: rename to snake_case, cast types, handle nulls.

`models/staging/stg_titanic.sql`:
```sql
with source as (
    select * from {{ source('raw', 'titanic') }}
),

renamed as (
    select
        "PassengerId"                           as passenger_id,
        "Pclass"                                as passenger_class,
        "Name"                                  as full_name,
        lower("Sex")                            as gender,
        coalesce("Age", 28.0)                   as age,
        "Survived"                              as survived,
        case
            when "Survived" = 1 then 'survived'
            else 'died'
        end                                     as survival_status,
        "Ticket"                                as ticket_number,
        round("Fare"::numeric, 2)               as fare_gbp,
        coalesce("Embarked", 'S')               as embarkation_code,
        case
            when "Embarked" = 'C' then 'Cherbourg'
            when "Embarked" = 'Q' then 'Queenstown'
            when "Embarked" = 'S' then 'Southampton'
            else 'Unknown'
        end                                     as embarkation_port,
        "SibSp"                                 as siblings_spouses,
        "Parch"                                 as parents_children,
        "SibSp" + "Parch"                       as family_size
    from source
)

select * from renamed
```

Reference in downstream models: `{{ ref('stg_titanic') }}`

---

## 8. Intermediate models

Add derived fields on top of staging:

`models/intermediate/int_passengers_enriched.sql`:
```sql
with stg as (
    select * from {{ ref('stg_titanic') }}
),

enriched as (
    select
        *,
        case
            when age < 13  then 'child'
            when age < 18  then 'teenager'
            when age < 60  then 'adult'
            else 'senior'
        end                                     as age_group,
        case
            when fare_gbp < 7.9   then 'budget'
            when fare_gbp < 14.5  then 'economy'
            when fare_gbp < 31.0  then 'standard'
            else 'premium'
        end                                     as fare_tier,
        case
            when family_size = 0 then true
            else false
        end                                     as is_solo_traveller,
        case
            when passenger_class = 1 then 'First'
            when passenger_class = 2 then 'Second'
            else 'Third'
        end                                     as class_label
    from stg
)

select * from enriched
```

---

## 9. Mart models

Final analytical tables — materialised as tables for query performance.

**fct_survival_by_class** — 3 rows, one per class:

| passenger_class | class_label | total_passengers | survival_rate_pct | avg_fare_gbp |
|---|---|---|---|---|
| 1 | First | 216 | 63.0 | 84.16 |
| 2 | Second | 184 | 47.3 | 20.66 |
| 3 | Third | 491 | 24.2 | 13.68 |

**fct_survival_by_demographics** — 10 rows across 3 dimensions:

| dimension | segment | survival_rate_pct |
|---|---|---|
| gender | female | 74.2 |
| gender | male | 18.9 |
| age_group | child | 58.0 |
| fare_tier | premium | 58.2 |

---

## 10. Generic tests

Defined in YAML — `unique`, `not_null`, `accepted_values`, `relationships`:

`models/staging/stg_titanic.yml`:
```yaml
version: 2

models:
  - name: stg_titanic
    columns:
      - name: passenger_id
        data_tests:
          - unique
          - not_null
      - name: passenger_class
        data_tests:
          - not_null
          - accepted_values:
              values: [1, 2, 3]
      - name: survived
        data_tests:
          - accepted_values:
              values: [0, 1]
      - name: gender
        data_tests:
          - accepted_values:
              values: ['male', 'female']
```

---

## 11. Singular tests

Custom SQL in `tests/` — must return 0 rows to pass:

`tests/test_survival_rate_bounds.sql`:
```sql
-- Survival rate must be between 0 and 100
select passenger_class, survival_rate_pct
from {{ ref('fct_survival_by_class') }}
where survival_rate_pct < 0
   or survival_rate_pct > 100
```

`tests/test_total_passengers_matches_source.sql`:
```sql
-- No rows lost in transformation
with source_count as (
    select count(*) as cnt from {{ source('raw', 'titanic') }}
),
mart_count as (
    select sum(total_passengers) as cnt from {{ ref('fct_survival_by_class') }}
)
select source_count.cnt as source_rows, mart_count.cnt as mart_rows
from source_count, mart_count
where source_count.cnt != mart_count.cnt
```

**Result: 26/26 tests passed in 0.56s**

---

## 12. Documentation

```bash
# Generate docs
dbt docs generate

# Serve on port 8082
dbt docs serve --port 8082 &

# Open in browser
wslview http://$(ip addr show eth0 | grep "inet " | awk '{print $2}' | cut -d/ -f1):8082
```

The docs site shows:
- Every model with its description and column list
- All tests attached to each column
- **Lineage graph** (blue button, bottom right) — visual DAG:

```
source: public.titanic
        ↓
stg_titanic (view)
        ↓
int_passengers_enriched (view)
        ↓          ↓
fct_survival_by_class    fct_survival_by_demographics
(table)                  (table)
```

---

## 13. Essential commands

```bash
dbt debug          # test connection
dbt run            # build all models
dbt test           # run all tests
dbt run && dbt test  # build then test — standard CI pattern

dbt run --select stg_titanic              # run one model only
dbt run --select staging.*               # run all staging models
dbt run --select +fct_survival_by_class  # run model + all ancestors
dbt test --select stg_titanic            # test one model only

dbt docs generate  # build documentation site
dbt docs serve --port 8082  # serve docs locally

dbt compile        # compile SQL without running — useful for debugging
dbt clean          # delete target/ folder
```

---

## 14. Key concepts

| Concept | Why it matters |
|---|---|
| `{{ source('raw', 'titanic') }}` | References a raw table — tracked in lineage graph |
| `{{ ref('stg_titanic') }}` | References another dbt model — dbt resolves build order automatically |
| `+materialized: view` | Staging as views — zero storage, always fresh |
| `+materialized: table` | Marts as tables — fast analytical queries |
| `dbt_dev` schema | dbt writes to its own schema — raw data never touched |
| Generic tests | `unique`, `not_null`, `accepted_values` — defined in YAML, no SQL needed |
| Singular tests | Custom SQL returning 0 rows = pass — for business-specific rules |
| Lineage graph | Auto-generated from `ref()` calls — shows full data flow visually |
| Three-layer architecture | staging → intermediate → marts — each layer has one job |
| `coalesce("Age", 28.0)` | Handle nulls in staging — not in marts |
| Snake_case columns | Rename PascalCase in staging — all downstream models use clean names |

---

## 15. Week 5 checklist

- [x] dbt-core 1.8.2 + dbt-postgres 1.8.2 installed
- [x] `profiles.yml` configured — port 5433, schema `dbt_dev`
- [x] `dbt debug` — all checks passed
- [x] `sources.yml` — raw titanic table registered
- [x] `stg_titanic` — PascalCase → snake_case, nulls handled, survival_status derived
- [x] `int_passengers_enriched` — age_group, fare_tier, is_solo_traveller, class_label
- [x] `fct_survival_by_class` — 3-row summary table, all class metrics
- [x] `fct_survival_by_demographics` — 10-row table across gender, age, fare dimensions
- [x] 26 generic tests — unique, not_null, accepted_values across all models
- [x] 2 singular tests — survival rate bounds, total passenger count integrity
- [x] `dbt docs generate` + `dbt docs serve` — lineage graph visible
- [x] Git commit

---

**Next:** [Week 6 — PySpark · DataFrames · Spark SQL · medallion architecture](../week6/README.md)