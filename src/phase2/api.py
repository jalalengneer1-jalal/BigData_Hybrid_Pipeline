import os
import subprocess
import sys
from pathlib import Path

from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, Field
from pymongo.errors import PyMongoError

from .aggregations import REPORTS, run_report
from .common import ROOT, database, maintenance_lock, serializable
from .jobs import definitions, run_job
from .queries import QUERIES, create_indexes, explain_comparison, run_query
from .views import refresh

app = FastAPI(title="Hybrid Big Data — Phase 2", version="2.0.0")


def execute(operation):
    try:
        with database() as db:
            return serializable(operation(db))
    except KeyError as exc:
        raise HTTPException(404, f"Unknown name: {exc.args[0]}") from exc
    except ValueError as exc:
        raise HTTPException(422, str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(409, str(exc)) from exc
    except PyMongoError as exc:
        raise HTTPException(503, "MongoDB operation failed; check server and configuration") from exc


class IngestRequest(BaseModel):
    input_path: str
    batch_size: int = Field(default=5000, gt=0, le=100000)


@app.get("/health")
def health():
    return execute(lambda db: {"status": "ok", "database": db.name, "mongo": db.client.admin.command("ping")["ok"] == 1})


@app.post("/ingest")
def ingest(request: IngestRequest):
    path = Path(request.input_path).expanduser().resolve()
    if not path.is_file() or path.suffix.lower() != ".csv":
        raise HTTPException(422, "input_path must be an existing CSV file on this server")
    allowed = os.getenv("INGEST_ALLOWED_ROOT")
    if allowed and not path.is_relative_to(Path(allowed).expanduser().resolve()):
        raise HTTPException(422, "CSV must be within INGEST_ALLOWED_ROOT")

    def operation(db):
        with maintenance_lock(db):
            result = subprocess.run([sys.executable, "-m", "src.main", "--input", str(path), "--batch-size", str(request.batch_size)], cwd=ROOT, capture_output=True, text=True, encoding="utf-8", errors="replace", env={**os.environ, "PYTHONIOENCODING": "utf-8"})
            if result.returncode:
                raise RuntimeError("Existing ingestion pipeline failed: " + (result.stderr or result.stdout)[-4000:])
            return {"status": "success", "engine": "existing file router", "output": result.stdout[-8000:], "refresh_required": True}
    return execute(operation)


@app.post("/indexes")
def indexes():
    return execute(lambda db: {"indexes": create_indexes(db)})


@app.post("/indexes/explain")
def explain():
    return execute(explain_comparison)


@app.get("/queries")
def queries():
    return {"queries": QUERIES}


@app.get("/queries/{name}")
def query(name: str, value: str | None = None, start: str | None = None, end: str | None = None, limit: int = Query(100, ge=1, le=1000)):
    return execute(lambda db: run_query(db, name, value, start, end, limit))


@app.get("/aggregations")
def aggregations():
    return {"aggregations": REPORTS}


@app.get("/aggregations/{name}")
def aggregation(name: str, limit: int = Query(100, ge=1, le=1000)):
    def operation(db):
        with maintenance_lock(db):
            return run_report(db, name, limit)
    return execute(operation)


@app.post("/refresh-mv")
def refresh_mv():
    return execute(refresh)


@app.get("/jobs")
def jobs():
    return execute(lambda db: {"jobs": definitions(), "recent_runs": list(db.phase2_job_runs.find({}).sort("started_at", -1).limit(20))})


@app.post("/jobs/{name}/run")
def job(name: str):
    return execute(lambda db: run_job(db, name))
