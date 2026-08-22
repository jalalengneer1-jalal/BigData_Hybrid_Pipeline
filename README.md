# Hybrid Big Data Data-Quality Pipeline

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
