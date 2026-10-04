import os
import threading
import time
from uuid import uuid4

from .aggregations import REPORTS, run_report
from .common import LOGS, database, maintenance_lock, now, save_report, serializable
from .views import refresh

JOBS = {
    "refresh_views": {"description": "Incrementally refresh both materialized views", "seconds": 3600, "env": "VIEW_REFRESH_SECONDS"},
    "daily_reports": {"description": "Save five aggregation reports from latest facts", "seconds": 86400, "env": "REPORT_INTERVAL_SECONDS"},
}


def definitions():
    output = []
    for name, spec in JOBS.items():
        seconds = int(os.getenv(spec["env"], str(spec["seconds"])))
        if seconds < 1:
            raise ValueError(f"{spec['env']} must be positive")
        output.append({"name": name, "description": spec["description"], "interval_seconds": seconds})
    return output


def run_job(db, name):
    if name not in JOBS:
        raise KeyError(name)
    run_id = uuid4().hex
    started = now()
    db[LOGS].insert_one({"_id": run_id, "name": name, "started_at": started, "status": "running"})
    try:
        if name == "refresh_views":
            result = refresh(db)
        else:
            with maintenance_lock(db):
                result = {report: run_report(db, report) for report in REPORTS}
                save_report("daily_reports", {"generated_at": now(), "reports": result})
        db[LOGS].update_one({"_id": run_id}, {"$set": {"finished_at": now(), "status": "success", "result": serializable(result)}})
        return {"run_id": run_id, "name": name, "status": "success", "result": result}
    except Exception as exc:
        db[LOGS].update_one({"_id": run_id}, {"$set": {"finished_at": now(), "status": "failed", "error": str(exc)}})
        raise


def scheduler(stop=None):
    stop = stop or threading.Event()
    schedule = definitions()
    due = {job["name"]: time.monotonic() for job in schedule}
    while not stop.is_set():
        for job in schedule:
            if stop.is_set():
                break
            if time.monotonic() >= due[job["name"]]:
                try:
                    with database() as db:
                        run_job(db, job["name"])
                except Exception as exc:
                    print(f"Job {job['name']} failed: {exc}", flush=True)
                due[job["name"]] = time.monotonic() + job["interval_seconds"]
        stop.wait(1)


if __name__ == "__main__":
    try:
        scheduler()
    except KeyboardInterrupt:
        pass
