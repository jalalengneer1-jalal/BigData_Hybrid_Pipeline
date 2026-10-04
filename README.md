# Hybrid Big Data Data-Quality Pipeline — Midterm + Final (Phase 2)

## Final Project (7 marks): Quick Start

Phase 2 adds queries/indexes/Explain, five aggregation reports, two incremental
materialized views, two scheduled jobs, and FastAPI. The midterm ingestion and
cleaning modules below are reused without changes. Historical 30M results below
describe the midterm evidence, not a new Phase 2 run.

Run all commands from the repository root. Python 3.10+ and a running MongoDB
server are required. Java 17 is required only when the existing router chooses Spark.

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
Copy-Item .env.example .env
# .env is a template; configuration is read from process environment variables.
$env:MONGO_URI = 'mongodb://127.0.0.1:27017'
$env:MONGO_DATABASE = 'bigdata_midterm'

# Ingest your actual CSV using the SAME midterm pipeline (skip if already ingested).
python -m src.main --input '<FULL_CSV_PATH>'
python -m src.phase2.cli indexes
python -m src.phase2.cli explain
python -m src.phase2.cli refresh-mv
python -m uvicorn src.phase2.api:app --host 127.0.0.1 --port 8000
```

Open `http://127.0.0.1:8000/docs` for Swagger and interactive testing.
In a second terminal with the same environment, start the scheduler:

```powershell
python -m src.phase2.cli scheduler
```

Keep this process running to execute jobs on schedule; use **one** scheduler.
Both jobs run once on startup, then refresh every 3,600 seconds and report every
86,400 seconds after the previous run finishes. Override with
`VIEW_REFRESH_SECONDS` / `REPORT_INTERVAL_SECONDS`. The API itself does not start
a scheduler. This separates scheduled execution from API worker restarts.

### Unified JSON API

| Method | Route | Function |
|---|---|---|
| GET | `/health` | Live MongoDB connection check |
| POST | `/ingest` | Existing `src.main` router and pipeline; body `{"input_path":"C:/data/orders.csv","batch_size":5000}` |
| POST | `/indexes` | Create or verify Phase 2 indexes |
| POST | `/indexes/explain` | Save three before/after `executionStats` comparisons |
| GET | `/queries` | List five named queries |
| GET | `/queries/{name}` | Run query with `value`, `start`, `end`, `limit` |
| GET | `/aggregations` | List five named reports |
| GET | `/aggregations/{name}` | Execute report with `limit` |
| POST | `/refresh-mv` | Incrementally synchronize facts and both views |
| GET | `/jobs` | Job intervals and recent execution logs |
| POST | `/jobs/{name}/run` | Run either job manually |

For `/ingest`, paths refer to the server's filesystem. Set `INGEST_ALLOWED_ROOT`
to restrict input paths. Bind to loopback for local evaluation. Large ingestion
and initial refresh are synchronous and may take time; configure an appropriate
client timeout. After ingestion, call `/refresh-mv` (or let the refresh job run).
Unknown names return 404, invalid input returns 422, concurrent maintenance returns
409, and MongoDB failures return 503. Money is returned as exact decimal strings.

### Five Practical Queries and Index Evidence

| Query name | Parameters | Supporting index |
|---|---|---|
| `customer_orders` | `value=customer_id`, optional dates | `(customer_id, order_date)` |
| `city_orders` | `value=city`, optional dates | `(city, order_date)` |
| `status_orders` | `value=status`, optional dates | `(status, order_date)` |
| `date_orders` | required `start`, `end` | `order_date` |
| `quality_orders` | `value=valid` or `corrected`, optional dates | Existing midterm `quality_status` index |

Dates are `YYYY-MM-DD`; intervals are **start inclusive, end exclusive**.
The normalized midterm ISO date strings preserve chronological ordering.
Results default to 100 rows, maximum 1,000. Examples (replace placeholder values
with values from your data):

```powershell
python -m src.phase2.cli query --name customer_orders --value '<CUSTOMER_ID>'
python -m src.phase2.cli query --name city_orders --value '<CITY>'
python -m src.phase2.cli query --name status_orders --value '<NORMALIZED_STATUS>'
python -m src.phase2.cli query --name date_orders --start 2025-01-01 --end 2026-01-01
python -m src.phase2.cli query --name quality_orders --value corrected
python -m src.phase2.cli explain
```

The Explain command selects real filter values from a validated order. The
**before** measurement forces `$natural: 1` (unindexed COLLSCAN baseline), creates
the indexes, then **after** forces the named compound index. This remains
repeatable when indexes already exist and does not drop midterm indexes. Full
plans, `executionTimeMillis`, `totalDocsExamined`, `totalKeysExamined`, and
`nReturned` are saved to `reports/phase2/explain.json`. Compare actual scan costs;
execution time alone depends on cache and machine load. Five Phase 2 indexes
include three compound query indexes and an incremental `last_updated_at` index.

### Five Aggregation Reports

```powershell
python -m src.phase2.cli aggregation --name sales_by_city
python -m src.phase2.cli aggregation --name top_products
python -m src.phase2.cli aggregation --name top_customers
python -m src.phase2.cli aggregation --name sales_by_day
python -m src.phase2.cli aggregation --name orders_by_status
```

Each report runs independently through CLI or API and uses real MongoDB
aggregation stages over synchronized `phase2_order_facts`. Numeric fields and
the original JSON items string are normalized once into Decimal128 and arrays.
Reports default to 100 groups, maximum 1,000. All monetary groups include
**currency**; different currencies are never added together. Order revenue is
`total_amount` including delivery; product revenue is item `total`, excluding
delivery. All validated/corrected order statuses are included, so these are order
value summaries rather than a claim of recognized paid revenue.

### Two Materialized Views and Incremental Refresh

`daily_sales_summary` stores `{day, currency}` order counts and revenue.
`top_products_summary` stores `{product_key, currency}` quantities and product revenue.
They are physical MongoDB collections populated from aggregation results.

Product keys use `sku:<value>` when a SKU exists. Existing validated items without
a SKU use `name:<normalized name>`; these remain separate from known-SKU groups.
Items without either identifier appear as `unknown:unidentified`. No SKU is guessed.

On the first refresh, existing validated orders are streamed in bounded batches
of 1,000. Later refreshes select records by the midterm's `last_updated_at`
watermark. Fingerprints avoid rewriting unchanged facts. Old **and** new day/SKU
groups are marked durably before replacing an order fact. Only affected groups
are aggregated and replaced/upserted; empty old groups are deleted. This handles
amount, date, product, and currency edits without double counting or rebuilding
the entire dataset each time. Repeated refreshes are idempotent.

Checkpoint advancement happens after every marked group is repaired. A retry
after an interrupted refresh resumes pending groups. Reports refuse to run while
pending repair markers exist. API ingestion, refreshes, and report jobs share a
MongoDB maintenance lock. Do not run the original ingestion CLI concurrently with
refresh, or edit/delete validated documents directly: Phase 2 follows the existing
insert/update pipeline, which does not emit deletion events. Initial loading of a
large dataset is expensive; subsequent refreshes are incremental.

The lock has no automatic expiry, so a lengthy initial job cannot accidentally
overlap another job. If a process crashes, first verify it has stopped, then
remove only the `_id: "maintenance_lock"` document in `phase2_state` using Compass
and rerun refresh. Keep checkpoints and dirty markers intact.

### Scheduled Jobs and Verification

`refresh_views` refreshes both views. `daily_reports` writes the five reports to
`reports/phase2/daily_reports.json`. Each attempt is logged in
`phase2_job_runs` with name, start/end, success/failure, result or error.
If MongoDB itself is unavailable, the scheduler logs the failure to the terminal
because a database execution log cannot be written. Manual runs:

```powershell
python -m src.phase2.cli run-job --name refresh_views
python -m src.phase2.cli run-job --name daily_reports
python -m src.phase2.cli jobs
python -m pytest tests -q
# Enable real integration checks; tests use isolated phase2_test_* databases.
$env:PHASE2_TEST_MONGO_URI = 'mongodb://127.0.0.1:27017'
python -m pytest tests -q
```

Integration tests cover actual MongoDB aggregations and Explain plans, exact
decimal arithmetic, currency separation, old-group cleanup after order changes,
idempotency, interruption/retry, locking, timed jobs, JSON routes, Swagger, and
ingestion through the unchanged midterm entry point. No dashboard is required.

Before delivery, ensure all changes are committed and pushed to the same GitHub
repository, and verify setup from this README with your own CSV input.

University Big Data Midterm Project

## Project Objective

Hybrid Raw-first ELT pipeline for dirty e-commerce orders using Python Batch, Apache PySpark, MongoDB, and MongoDB Spark Connector.

## Prerequisites and Setup

- Python 3.10
- Java 17 for the PySpark path
- MongoDB running locally, or a reachable MongoDB URI

Create and activate a Python environment, then install the pinned dependencies:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

The tested Windows Spark setup uses Java 17. Set `JAVA_HOME` to the Java 17
installation (or make `java` available on `PATH`). A Conda Java runtime under
`Library\bin\java.exe` is also detected automatically. The Python Batch path does
not require Java. Start MongoDB before a real load; the defaults are:

```text
MONGO_URI=mongodb://127.0.0.1:27017
MONGO_DATABASE=bigdata_midterm
```

Optional environment overrides are `MONGO_URI`, `MONGO_DATABASE`,
`SMALL_FILE_THRESHOLD_MB`, `BATCH_SIZE`, and `SPARK_MASTER`.

The large source CSV is intentionally excluded from Git. Put the instructor's
unchanged file anywhere accessible and pass its full path to `--input`.

Create the required reproducible small sample without editing the source file:

```powershell
python -m src.create_small_sample --input "<LARGE_CSV_PATH>" --rows 100000 --output ".\data\samples\orders_small_sample.csv"
```

Verify the installation before the practical demo:

```powershell
python -m pytest tests -q
```

## Architecture

Dirty CSV -> File Router -> Python Batch (small) / PySpark (large) -> orders_raw -> Cleaning & Validation -> Valid / Corrected / Quarantined -> orders_validated / orders_quarantine -> Metrics

## Routing

Default threshold: 200 MB

- File <= 200 MB: Python Batch
- File > 200 MB: PySpark

The Router prints file size, threshold, selected engine, and selection reason.

## Main Command

python -m src.main --input "<CSV_PATH>"

Safe modes:

python -m src.main --input "<CSV_PATH>" --dry-route
python -m src.main --input "<CSV_PATH>" --raw-only

## Python Batch Path

- Python csv module
- Streaming CSV
- No list(reader)
- No full-file Pandas loading
- Configurable batch size
- MongoDB insert_many
- Tested batch size: 5,000

## PySpark Path

- SparkSession
- DataFrame API
- Explicit String schema
- MongoDB Spark Connector
- Parallel partitions

Official large run:
- File size: 12,650.32 MB
- Rows: 30,000,000
- Input partitions: 99
- Output partitions: 99
- CSV corrupt rows: 0
- Raw ingestion elapsed: 636.59 sec
- Raw throughput: 47,126.31 records/sec
- run_id: pipeline-20260816T235754Z-866233e7

## Raw-first ELT

Every record is inserted into orders_raw before cleaning. Raw metadata includes run_id, source file/path, ingestion timestamp, engine, and raw record. Dirty source values are preserved as strings.

## Quality Classification

Every Raw record becomes exactly one logical result:
- Valid
- Corrected
- Quarantined

Corrections are deterministic only. Unsafe or ambiguous records are quarantined instead of guessed.

## Cleaning Rules

Implemented rules include Arabic-digit normalization, decimal/thousand separator normalization, known price words, currency normalization, Yemen phone normalization, repeated email-symbol repair, date normalization, status synonym normalization, negative quantity derivation, item price/total derivation, and order-total recalculation.

Corrected records preserve an audit trail with field, original, corrected, and rule_code.

## Core Quarantine Codes

- MISSING_ORDER_ID
- MISSING_CUSTOMER_ID
- INVALID_IMPOSSIBLE_DATE
- CORRUPTED_ITEMS_JSON
- EMPTY_ITEMS
- UNKNOWN_PRICE
- AMBIGUOUS_NEGATIVE_VALUE
- DUPLICATE_ORDER_ID
- MULTIPLE_CONFLICTING_ERRORS

## Business Key and Idempotency

Stable business key: order_id

orders_validated has a unique index on order_id. Upsert is used so repeated processing does not create duplicate validated records.

## Official 100K Evidence

run_id: run-20260816T195634Z-2294f5ec

- Raw: 100,000
- Valid: 70,002
- Corrected: 21,697
- Quarantined: 8,301
- Consistency: 100,000 = 70,002 + 21,697 + 8,301

Preserved evidence collections:
- orders_validated_100k_evidence: 91,699
- orders_quarantine_100k_evidence: 8,301

## Official 30M Result

- Raw: 30,000,000
- Valid: 20,994,411
- Corrected: 6,501,781
- Quarantined: 2,503,808
- orders_validated: 27,496,192
- orders_quarantine: 2,503,808
- Consistency: PASS
- Quality ELT elapsed: 28,965.12 sec
- Quality ELT throughput: 1,035.73 records/sec

## Bounded-Memory Processing

The 30M ELT does not materialize the full MongoDB final state in Python RAM. Existing-state lookup is bounded to 2,000 keys per Raw batch using MongoDB queries.

## Final Reports

- reports/results.json
- reports/results.md
- reports/final_verification.json
- reports/spark_large_run_final.json
- reports/elt_write_report_large_30m_final.json
- reports/classification_dry_run.json
- reports/elt_write_report_final_idempotency.json
- reports/upsert_update_proof.json

## Testing

Run:

python -m pytest tests -q

Final verified result: 53 tests passed.

## Final Core Status

- File Router: PASS
- Python Batch: PASS
- PySpark PASS
- MongoDB Spark Connector: PASS
- Raw-first: PASS
- Cleaning rules: PASS
- Valid / Corrected / Quarantine: PASS
- Correction audit trail: PASS
- Upsert: PASS
- Idempotency: PASS
- 100K evidence preserved: PASS
- 12.35 GB execution: PASS
- 30M processing: PASS
- Final consistency: PASS
- Final metrics: PASS
- Automated tests: PASS

## Design Decisions and Rationale

The engineering rationale behind routing, the 200 MB threshold, Raw-first ELT,
fixed String schemas, deterministic corrections, quarantine, Upsert/idempotency,
bounded-memory processing, and the Spark design is documented in:

`docs/DESIGN_DECISIONS.md`

This document is also intended as a technical reference for the project viva.
