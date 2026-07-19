# Junior Data Engineer — Interview Preparation Guide

Based on 7 weeks of hands-on work. Every answer references something you actually built.

---

## Technical questions — SQL

**Q: What's the difference between RANK() and DENSE_RANK()?**

RANK() leaves gaps after ties — if two rows share rank 1, the next rank is 3. DENSE_RANK() has no gaps — the next rank is 2. I used both in week 2 when ranking Titanic passengers by fare within class. Three passengers all paid £512, so they shared rank 1. With RANK() the next passenger was rank 4; with DENSE_RANK() they were rank 2. In DE work I use DENSE_RANK() for deduplication (keep the latest record per customer) and RANK() for competition-style rankings.

**Q: What is a window function and when would you use one?**

A window function computes a value for each row based on a set of related rows, without collapsing them like GROUP BY does. I use them for: running totals (cumulative revenue), moving averages (smoothing time series), period-over-period comparisons with LAG/LEAD (day-over-day change), deduplication with ROW_NUMBER (keep first record per group), and rankings. I built all of these in week 2 against a real PostgreSQL database.

**Q: How would you optimise a slow SQL query?**

First I'd run EXPLAIN ANALYZE to see the query plan. I look for Seq Scan on large tables — that means no index is being used. I'd add an index on the filter column, then check if the plan switches to Index Scan. I'd also check if the query is pulling more columns than needed (SELECT * is expensive at scale — push the projection to SQL). In week 2 I demonstrated this: adding an index on the Fare column reduced execution time from 0.98ms to 0.047ms — a 21x improvement.

**Q: What's a CTE and when would you use a recursive one?**

A CTE (Common Table Expression) is a named temporary result set defined with WITH. Regular CTEs improve readability and let you reference the same subquery multiple times. Recursive CTEs traverse hierarchical data — I used one in week 2 to walk an org chart from CEO down through all reports, building a path string at each level. In DE work recursive CTEs appear in bill-of-materials processing, folder tree traversal, and network graphs.

---

## Technical questions — Python & pipelines

**Q: What's the difference between pandas and PySpark?**

pandas loads everything into RAM on one machine — fast for small data, falls over at scale. PySpark distributes data across partitions and processes them in parallel across many machines. The key conceptual difference is lazy evaluation: PySpark builds a query plan and only executes it when you call an action like .show() or .write(). This lets the optimiser reorder operations, push filters down, and skip columns before touching disk. I built medallion pipelines in both — pandas for the weekly batch pipeline and PySpark for the scalable version partitioned by year.

**Q: How do you handle missing data in a pipeline?**

It depends on the column and business context. In my Chicago inspections pipeline: violations were null for ~2,100 rows because passed inspections often have none — I filled those with 0 and documented why. City and state had a handful of nulls — I filled with 'CHICAGO' and 'IL' as safe defaults for this dataset. Coordinates were null for 50 rows — I left those as null because imputing location data would be misleading. The key is: document every decision in your staging model so the next engineer knows why.

**Q: What is SQLAlchemy and why use it instead of psycopg2 directly?**

SQLAlchemy is an abstraction layer over database drivers like psycopg2. It provides connection pooling (reuse connections instead of creating one per query — expensive at scale), a context manager for atomic transactions (automatic rollback on failure), parameterised queries (SQL injection prevention), and a unified API across different databases. I built a reusable db_utils.py module in week 2 with all of these patterns, which I reused in weeks 3, 4, and 5.

---

## Technical questions — data engineering tools

**Q: What is dbt and what problem does it solve?**

dbt handles the T in ELT — transforming data that's already in your warehouse using SQL. It solves three problems: version control for SQL (transformations live in Git, not scattered scripts), automated testing (I ran 26 tests in 0.56 seconds catching nulls, duplicates, and business rule violations), and documentation with lineage graphs showing exactly how data flows from source to mart. In my capstone I used dbt to transform raw Chicago inspection data through staging → intermediate → three mart tables, with 9 automated tests.

**Q: Explain the dbt ref() function.**

ref() creates a dependency between models and tells dbt the build order. Instead of hardcoding a table name, you write {{ ref('stg_titanic') }} — dbt resolves this to the correct schema and table name, ensures stg_titanic is built before any model that references it, and tracks the dependency in the lineage graph. Without ref() you'd have to manually order your SQL scripts and hardcode schema names. With ref() dbt handles all of that automatically.

**Q: What is Apache Airflow and how does it differ from cron?**

Airflow is a workflow orchestration platform. Cron just runs scripts on a schedule with no awareness of success or failure. Airflow adds: task dependencies (don't run task B until task A succeeds), automatic retries with configurable delays, a UI showing every run's history and logs, branching (take different paths based on data), sensors (wait for conditions before proceeding), and XComs (pass data between tasks). I built DAGs in week 4 with all of these, including a BranchPythonOperator that routed to different handlers based on row count.

**Q: What is the medallion architecture?**

Three layers in a data lakehouse: Bronze is raw data as ingested — never modified, it's your audit trail. Silver is cleaned and validated — consistent types, nulls handled, duplicates removed, DQ checks run here. Gold is aggregated and business-ready — one table per use case, optimised for analytical queries. Analysts query gold, never bronze. I implemented this in both week 6 (Titanic) and week 7 (Chicago inspections) with PySpark, writing partitioned Parquet at each layer.

**Q: Why use Parquet instead of CSV?**

Three reasons. First, columnar storage: if a query only needs 3 of 200 columns, Parquet reads only those 3 columns from disk — CSV always reads everything. Second, compression: Parquet uses Snappy or Gzip compression, typically 2-5x smaller than CSV. Third, schema embedded: data types are stored in the file, so you don't guess whether a column is a string or integer. I benchmarked this in week 1 and demonstrated column pruning — reading 3 columns from a Parquet file was faster than reading all columns even though the file was smaller.

---

## Technical questions — Docker & infrastructure

**Q: What is Docker Compose and why use it?**

Docker Compose defines a multi-service stack in a single YAML file. Instead of running separate docker run commands for Postgres, pgAdmin, Airflow webserver, Airflow scheduler, and your pipeline container — each with the right ports, volumes, environment variables, and startup dependencies — you define it all once and run docker compose up -d. Every engineer gets the identical environment. I used this from week 3 onwards, running stacks with 3-5 services.

**Q: What is a Docker volume and why does it matter?**

A volume is storage that lives outside the container — it persists when the container is removed. Without volumes, all database data disappears when you stop a container. I demonstrated this in week 3: inserted data, ran docker compose down (destroyed all containers), then docker compose up (recreated them), and the data was still there. In production this is critical — you'd never run a database without a named volume.

**Q: How do containers communicate with each other in Docker?**

Docker Compose puts all services on a shared internal network. Services resolve each other by service name — if your Postgres service is named de-postgres, other containers connect using host=de-postgres, not localhost. localhost inside a container means the container itself. This was the most common source of bugs in week 4 when connecting Airflow to the data database — I had to bridge two separate Compose networks and use the correct service name.

---

## Behavioural questions

**Q: Tell me about a complex technical problem you solved.**

In week 4, I was connecting Apache Airflow to a PostgreSQL database from a previous week's Docker Compose stack. The connection kept failing with authentication errors even though the credentials were correct. I read the task logs systematically, found the actual error (host.docker.internal resolving to a Docker Desktop IP instead of my WSL2 container), then diagnosed that two Compose stacks had a hostname conflict — both had a service called postgres, and attaching both networks caused Airflow to connect to the wrong one. I fixed it by renaming Airflow's metadata DB service and explicitly bridging the networks. The debugging process taught me more about Docker networking than any tutorial.

**Q: How do you ensure data quality in a pipeline?**

Multiple layers. At ingestion: validate row counts, check for null primary keys, detect duplicates before loading. In the transformation layer: dbt tests run on every model — I had 26 tests in the Titanic project and 9 in the Chicago capstone, covering uniqueness, null constraints, accepted values, and custom business rules. In the pipeline runner: I log every run to a pipeline_runs table with status, row count, start and end time — so failures are visible immediately. For monitoring: I built a data_quality_log table that records every individual check result with a timestamp.

**Q: Describe your development process for a new pipeline.**

I start with the data — load it as-is, look at nulls, distributions, and anomalies. Then I define the business questions the pipeline needs to answer. I build incrementally: raw ingestion first, then staging (clean and rename), then intermediate (business logic), then marts (aggregations). I write tests at each layer before moving to the next. I containerise with Docker so it runs identically in any environment. I add an Airflow DAG for scheduling and observability. Finally I document: a README explaining what the data shows, not just how the pipeline works.

---

## System design questions

**Q: Design a pipeline that processes 1 billion rows of transaction data daily.**

I'd use a medallion architecture on a cloud data lake. Raw files land in S3 as Bronze (append-only, never modified). A PySpark job on EMR or Databricks reads Bronze, applies cleaning and validation, writes partitioned Parquet to Silver (partitioned by date so each day's data is in one folder — queries skip irrelevant partitions). A second Spark job aggregates Silver into Gold tables in a data warehouse like Snowflake or BigQuery. Airflow orchestrates the daily schedule with retries and alerting. dbt manages the warehouse transformations with automated testing. The key design decisions: partition by date (most queries filter by date), use columnar Parquet (column pruning reduces I/O dramatically), run DQ checks in Silver before promoting to Gold.

**Q: How would you handle late-arriving data?**

Partition by event date, not load date. When late data arrives, reprocess only the affected date partition (overwrite mode). In Airflow, use catchup=True for historical backfills and configure the DAG to reprocess a configurable lookback window. In the data warehouse, use MERGE statements instead of INSERT to upsert late-arriving records. Track a last_updated timestamp on every row so consumers know when data was last refreshed.

---

## Questions to ask the interviewer

- How is the data engineering team structured — embedded in product teams or centralised?
- What does the current data stack look like, and what's planned to change?
- How do you handle data quality issues discovered in production?
- What does a typical first project look like for a junior DE?
- How do you balance pipeline reliability with iteration speed?
- What observability do you have on your pipelines today?