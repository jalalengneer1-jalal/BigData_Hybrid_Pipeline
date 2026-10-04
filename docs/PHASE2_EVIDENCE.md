# Final Project Evidence — Phase 2

Verified locally on 2026-10-05 (Asia/Aden). The additions implement the final
assignment only; existing midterm source modules are unchanged.

| Requirement | Implementation | Verification |
|---|---|---|
| Five queries | `src/phase2/queries.py` | Real MongoDB query results and API integration checks |
| Three or more indexes including compound | Three equality/date compound indexes, date index, update index | Index creation is repeatable |
| Three Explain comparisons | CLI `explain`, POST `/indexes/explain` | `reports/phase2/explain.json`, full executionStats |
| Five aggregation reports | `src/phase2/aggregations.py` | Actual outputs in `reports/phase2/daily_reports.json` |
| Two materialized views | `src/phase2/views.py` | `daily_sales_summary`, `top_products_summary` |
| Incremental view refresh | Timestamp watermark + old/new dirty groups + fact fingerprints | Updates, moves, retry and idempotency integration tests |
| Two scheduled jobs | `src/phase2/jobs.py` | Timed dispatch and manual execution tests; persisted logs |
| Unified FastAPI | `src/phase2/api.py` | Every required route, Swagger, and existing ingestion tested |
| Setup and documentation | README, `.env.example`, requirements | Commands documented for CLI and API |

## Actual Local Data

Database: `bigdata_midterm`. This verification uses the current local validated
data, not the historical 30M midterm report.

- Validated source orders: 91,699.
- Normalized analytics facts: 91,699.
- Daily materialized-view groups: 121.
- Product materialized-view groups: 12.
- Sum of daily order counts: 91,699 (consistency PASS).
- Repeated refresh: 0 scanned/changed orders and 0 dirty groups.
- Both manual jobs completed successfully.
- Five real aggregation outputs were saved. API/CLI group limits are explicitly
  included in each response; default 100 may truncate a report such as daily sales.

## Explain Evidence

The baseline deliberately forces `$natural: 1` rather than dropping pre-existing
indexes. The indexed measurement forces the relevant named compound index.

| Query | Baseline documents examined | Indexed documents examined | Returned in both runs |
|---|---:|---:|---:|
| customer_orders | 91,699 | 1 | 1 |
| city_orders | 91,699 | 100 | 100 |
| status_orders | 91,699 | 100 | 100 |

These values reflect actual executionStats for selected existing data. They are
evidence, not constants used by application logic. Results depend on the input.

## Automated Checks

- Full suite: 59 passed (53 existing checks + 6 Phase 2 integration/unit checks).
- All six Phase 2 checks passed again after optimizing duplicate dirty-group
  writes within each bounded batch.
- Tests use isolated `phase2_test_*` databases and remove them afterward.
- Original ingestion reports are preserved/restored by the API ingestion test.
- No full new 30M run was performed; scalable bounded processing is implemented,
  but the actual Phase 2 run above is the 91,699-order local dataset.

## Operational Notes

Reports reflect the latest successful facts refresh. Run refresh after ingestion.
The existing source pipeline emits inserts/updates, not deletions; direct deletes
or writes outside it are not tracked. Do not run standalone ingestion concurrently
with refresh. API-mediated ingestion and maintenance use a shared MongoDB lock.
After a crashed process, verify it stopped before clearing only its maintenance
lock, then retry refresh. Dirty groups and checkpoint records must be retained.

Git remote points to the original repository. Large input CSV files, local
environment files, and credentials are excluded from version control.
