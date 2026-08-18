# turbine_etl

A PySpark/Delta Lake ETL pipeline that turns raw wind turbine sensor readings into validated,
analysis-ready tables using a medallion (bronze/silver/gold/summary) architecture.

## 1. Overview

### 1.1 Problem

The company operates a wind turbine farm of 15 turbines. Sensor data arrives as 3 CSV files,
each covering a fixed set of 5 turbines (file 1 → turbines 1-5, file 2 → 6-10, file 3 → 11-15),
with three metrics per reading: wind speed, wind direction, and resulting power output (MW).

Once every 24 hours (at some point after 00:00), each CSV is appended with the previous day's
24 hourly readings (00:00-23:00) for its 5 turbines. Sensors can occasionally miss individual
readings. The CSVs are cumulative — they already hold a full month of history by the time this
pipeline runs, and keep growing daily. The CSV structure itself is assumed stable and never
changes shape.

The task is to build a pipeline that processes this data through the bronze → silver → gold →
summary layers on each run, applying schema and data-quality validation along the way, and
producing a rolling per-turbine summary (min/max/average power output).

### 1.2 Architecture

- **Processing engine**: PySpark / Spark SQL, chosen specifically to demonstrate a distributed
  data-processing approach (partitioned DataFrame transformations, SQL-style aggregations)
  rather than single-machine pandas-style scripting.
- **Storage**: [Delta Lake](https://delta.io/) tables, not a relational database and not plain
  Parquet. Delta gives ACID writes, schema enforcement, and (in future) upsert/merge support,
  purely as files on disk plus a transaction log — i.e. relational-database-like guarantees
  without running an actual database server.
- **Pattern**: medallion architecture —
  - **Bronze** (`turbine_raw`) — raw CSV contents, schema-validated but otherwise untouched
  - **Silver** (`turbine_curated`) — bronze data quality-checked against the expected daily
    shape (15 turbines × 24 hours)
  - **Gold** (`turbine_conformed`) — silver data conformed into the canonical output shape,
    with a generated primary key and pipeline run timestamp, checked for duplicates and
    statistical anomalies
  - **Summary** (`summary_stats`) — rolling per-turbine min/max/average power output over the
    trailing 2 days, appended each run
- **Orchestration**: `turbine_etl/run_etl.py` builds one Spark session, instantiates the four
  Delta tables if they don't exist, and runs the four layer transformers in order.

### 1.3 Key design decisions and assumptions

A few points required an explicit judgement call rather than being fully specified up front:

- **Delta Lake over plain Parquet or a relational DB.** Plain Parquet has no transaction log,
  no schema enforcement, and no upsert support — all of which matter here. A relational
  database (via JDBC) would give real engine-enforced constraints, but is an awkward fit for a
  Spark-native bronze/silver/gold pipeline. Delta was the closest match to what "bronze/silver/
  gold" conventionally means, with no separate database server to run.
- **Bronze and silver share one schema.** Both layers hold the exact same five columns
  (`timestamp`, `turbine_id`, `wind_speed`, `wind_direction`, `power_output`), so `schema.py`
  defines this once as `PRE_CONFORMED_SCHEMA` rather than duplicating it.
- **"Pipeline run date" is trusted, not derived from the data.** Per the stated assumption that
  the pipeline runs immediately after the previous day's data lands, `Curate` and `StatsSummary`
  treat `target_date = pipeline_run_date - 1` (defaulting to the real current date, but
  overridable for testing). If no rows exist for that date at all — e.g. when re-running against
  older historical data — `Curate` treats that as "nothing new landed today" rather than a data
  quality failure: it skips the missing/unexpected-entry checks for that run and still promotes
  the full existing bronze history through to silver unchanged, instead of halting the whole
  pipeline over an empty day.
- **The "unexpected entries" check uses the composite key, not `measurement_id`.** The
  `measurement_id` field doesn't exist until the gold layer, so `Curate`'s completeness check
  (which runs against silver) validates against the `(turbine_id, timestamp)` composite key
  instead — functionally equivalent, but available at the layer where the check actually runs.
- **`DuplicateEntryError` fires on duplicates, not on missing entries.** The original checklist
  wording for this check was inverted; the implementation follows the evident intent.
- **A single `DataRangeSchemaError`.** Range violations are checked (and raise the same error)
  at both the bronze stage (`Ingest`) and the silver stage (`Curate`), sharing one validation
  helper, rather than defining separate strict/lenient variants.
- **Error classes carry structured metadata**, not just a message: `log_level`,
  `handling_method` (`raise`, `input`, `removal`, `raise_only`), and a `message_template` per
  error type, defined once in `error_classes.py`. **Not yet wired in**: the `input`/`removal`
  auto-remediation behaviour these describe (e.g. inserting a null row for a missing entry, or
  dropping a single bad row and continuing) is not yet implemented in the transformers — every
  error currently halts the run rather than self-healing. This is the main known gap against the
  original spec.
- **Sample, not population, standard deviation.** The anomaly check in `Conform` uses Spark's
  default `stddev` (sample stddev), which shifts the ±2σ threshold slightly versus population
  stddev, especially on small turbine samples.

## 2. Repository structure

```
turbine_etl_repo/
├── raw_data/                    # Source CSVs (data_group_1/2/3.csv), appended to daily
├── etl_outputs/
│   ├── turbine_db/              # The 4 Delta tables, created here from scratch by the pipeline, if non-existent
│   │   ├── turbine_raw/         # Bronze
│   │   ├── turbine_curated/     # Silver
│   │   ├── turbine_conformed/   # Gold
│   │   └── turbine_summary/     # Summary stats
│   └── etl_history/
│       ├── <timestamp>.log      # One logfile per pipeline run
│       └── pipeline_metadata.json  # Last run's per-CSV row counts (for before/after logging)
├── turbine_etl/                 # The installable package
│   ├── schema.py                 # Table schemas as dicts: {dtype, nullable, range} per field
│   ├── error_classes.py          # Custom exceptions, each with log_level/handling_method/message_template
│   ├── transformers.py           # Ingest, Curate, Conform, StatsSummary — one class per layer
│   ├── run_etl.py                # Entry point: builds Spark session, runs the layers in order
│   ├── logger_setup.py           # Per-run timestamped logfile setup
│   └── etl_metadata.py           # Persists CSV row counts between runs
├── test/                         # pytest suite — see below
├── .github/workflows/
│   ├── pre_commit.yml            # Runs on every push, any branch
│   └── deploy_dev.yml            # Runs on PRs into development
├── .pre-commit-config.yaml       # Local git pre-commit hook definition
├── .flake8                       # max-line-length = 200
├── pyproject.toml                # Package metadata + dependencies; enables `pip install -e .`
└── requirements.txt               # Pinned runtime + dev dependencies
```

**The script that gets executed is `turbine_etl/run_etl.py`.** Its `main()` function is the
starting point for reading the whole pipeline: it builds the Spark session, instantiates the
Delta tables, then calls `Ingest`, `Curate`, `Conform`, and `StatsSummary` in that order from
`transformers.py`. From there, the logic can be followed by tracing which function each call
leads into — each transformer class has a docstring describing what layer it produces, and its
`run()` method is the entry point into that layer's own read → validate → write sequence.

### What the tests assess

Each transformer has its own `test_xxx.py` file, all sharing fixtures from `test/conftest.py`:
a single dummy day of clean data (15 turbines × 24 hours = 360 rows), and isolated temp-directory
stand-ins for the 4 Delta tables so tests never touch `etl_outputs/turbine_db/`. Each test file
exercises that transformer's actual data-quality checks against the dummy dataset — for example,
`test_curate.py` seeds a dummy bronze table missing one row and asserts `MissingEntryError` is
raised, or with a duplicated row and asserts `UnexpectedEntryError` is raised. They're
deliberately narrow: covering the core pass/fail behaviour of each DQ check, not an exhaustive
matrix of every possible input.

## 3. Committing and merging

Two separate mechanisms enforce the same checks (flake8 with a 200-character line limit, and
pytest) at different points:

- **Committing to a feature branch**: the local git hook defined in `.pre-commit-config.yaml`
  runs automatically on `git commit` (once installed — see setup below) and **blocks the commit
  outright** if flake8 or pytest fail. This is genuinely local and pre-commit; it never reaches
  GitHub if it fails.

- **Merging a feature branch into `development`**: `.github/workflows/deploy_dev.yml` triggers
  on pull requests targeting `development` and runs flake8 + pytest again, plus a `black`
  formatting check that — if the code isn't already black-formatted — opens a *separate* pull
  request with the formatting fix applied, rather than failing the check outright. As with the
  push-triggered workflow, this only reports status; making it actually block the merge button
  requires enabling "require status checks to pass" in the repo's branch protection settings for
  `development`, which is a GitHub setting, not something expressed in the workflow file.

## 4. Running the pipeline

### 4.1 Environment

This project is built to run in a Linux terminal environment (WSL2 on Windows, or native Linux/
macOS). PySpark's local worker model, and the Hadoop filesystem shims it depends on even in
local (non-cluster) mode, are native to Linux; on Windows they need extra manual setup —
`JAVA_HOME`, a `HADOOP_HOME` pointing at manually-downloaded `winutils.exe`/`hadoop.dll`, and
`PYSPARK_PYTHON` explicitly set — none of which is needed on Linux. In practice, local
development on this repo ended up done via Git Bash with a natively-installed Windows Python
(WSL1 wasn't compatible with the VS Code setup used, and WSL2 wasn't available on this machine),
which required all of the above manual steps plus surfaced a known PySpark 3.5.3 /
Python 3.12 / Windows-specific worker-crash issue. **If you're setting this up fresh, use WSL2
or a native Linux/macOS environment** to avoid this entirely.

### 4.2 IDE / environment setup

1. Clone the repo and open it in your IDE.
2. Create and activate a virtual environment with **Python 3.11** (not 3.12 — see the callout
   below):
   ```
   python -m venv .venv
   source .venv/bin/activate        # Linux/macOS/WSL
   # or: source .venv/Scripts/activate   # Git Bash on Windows
   ```

   > **Why 3.11, not 3.12**: PySpark 3.5.3 has a worker-crash bug on Python 3.12, confirmed on
   > this project on Windows — `Python worker exited unexpectedly (crashed)` /
   > `java.io.EOFException`, and it isn't limited to one operation. It was reproduced on a bare
   > `spark.createDataFrame(...).count()`, on the test suite (any test seeding a Delta table via
   > `spark.createDataFrame`), and on the real pipeline itself (`Ingest`'s very first Delta
   > write, i.e. before it even reaches the raw CSVs). Python 3.12 currently cannot run this
   > project at all on Windows, not just "with reduced functionality." `pyproject.toml` pins
   > `requires-python` to `>=3.11,<3.12` for exactly this reason — `pip install -e .` will refuse
   > to install on 3.12 rather than silently installing into a broken environment. The issue
   > wasn't reproduced on Linux; if you're on WSL2/native Linux you may not hit it, but 3.11 is
   > the known-good version either way.
3. Install Java (PySpark runs on the JVM) — a JDK 17 distribution such as Temurin. On Linux:
   `sudo apt install openjdk-17-jdk`. On Windows without WSL, you'll additionally need Hadoop's
   `winutils.exe`/`hadoop.dll` and a `HADOOP_HOME` pointing at them.
4. Install the project in editable mode, which pulls in every dependency (`pyspark`,
   `delta-spark`, `pytest`, `flake8`, `pre-commit`) and makes `turbine_etl` importable:
   ```
   pip install -e .
   ```
5. Activate the local pre-commit hook once per clone:
   ```
   pre-commit install
   ```

### 4.3 Environment variables

The pipeline doesn't currently require any environment variables or secrets — all paths are
relative to the repo root and there's no external service to authenticate against. A `.env`
(gitignored) modelled on a `.env.example` is anticipated for when this moves toward a live
deployment (e.g. cloud storage credentials, a scheduler's config) — that file doesn't exist yet
because there's nothing real to put in it.

### 4.4 Running the tests

Once the project is installed (`pip install -e .` above, which is what makes `turbine_etl`
importable as a package rather than needing path hacks), run the suite from the repo root:

```
pytest
```

If you see `Python worker exited unexpectedly (crashed)`, you're on Python 3.12 — see the
callout in section 4.2.

### 4.5 Running the pipeline

```
python -m turbine_etl.run_etl
```

This builds a Spark session, creates the four Delta tables under `etl_outputs/turbine_db/` if
they don't already exist, runs `Ingest → Curate → Conform → StatsSummary` in order, and writes a
timestamped run log to `etl_outputs/etl_history/`.

### 4.6 Deploying via `main`

Changes reach `main` via the same PR-based process as merging into `development` (see section 3).
`main` is intended to be the branch a live/scheduled environment deploys from — the goal being
an automated run of `python -m turbine_etl.run_etl` every 24 hours against that day's freshly
appended CSVs. **That scheduling/deployment infrastructure isn't set up yet** — there's no
`deploy_main.yml` or scheduler configured in this repo; this section describes the intended
target, not current behaviour.
