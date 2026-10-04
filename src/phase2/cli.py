import argparse
import json

from .aggregations import REPORTS, run_report
from .common import database, json_default, maintenance_lock
from .jobs import JOBS, definitions, run_job, scheduler
from .queries import QUERIES, create_indexes, explain_comparison, run_query
from .views import refresh


def main():
    parser = argparse.ArgumentParser(description="Final-project commands")
    parser.add_argument("command", choices=["indexes", "explain", "query", "aggregation", "refresh-mv", "jobs", "run-job", "scheduler"])
    parser.add_argument("--name")
    parser.add_argument("--value")
    parser.add_argument("--start")
    parser.add_argument("--end")
    parser.add_argument("--limit", type=int, default=100)
    args = parser.parse_args()
    if args.command == "scheduler":
        scheduler()
        return
    with database() as db:
        if args.command == "indexes":
            result = create_indexes(db)
        elif args.command == "explain":
            result = explain_comparison(db)
        elif args.command == "query":
            if args.name not in QUERIES:
                parser.error("--name must be one of " + ", ".join(QUERIES))
            result = run_query(db, args.name, args.value, args.start, args.end, args.limit)
        elif args.command == "aggregation":
            if args.name not in REPORTS:
                parser.error("--name must be one of " + ", ".join(REPORTS))
            with maintenance_lock(db):
                result = run_report(db, args.name, args.limit)
        elif args.command == "refresh-mv":
            result = refresh(db)
        elif args.command == "run-job":
            if args.name not in JOBS:
                parser.error("--name must be one of " + ", ".join(JOBS))
            result = run_job(db, args.name)
        else:
            result = definitions()
    print(json.dumps(result, default=json_default, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
