import json
import os
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from bson import Decimal128, ObjectId
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError

ROOT = Path(__file__).resolve().parents[2]
FACTS = "phase2_order_facts"
DAILY = "daily_sales_summary"
PRODUCTS = "top_products_summary"
STATE = "phase2_state"
DIRTY = "phase2_dirty_groups"
LOGS = "phase2_job_runs"


def now():
    return datetime.now(timezone.utc)


def json_default(value):
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, (Decimal128, ObjectId)):
        return str(value)
    raise TypeError(type(value).__name__)


def serializable(value):
    return json.loads(json.dumps(value, default=json_default, ensure_ascii=False))


def save_report(name, value):
    directory = ROOT / "reports" / "phase2"
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"{name}.json"
    temporary = path.with_suffix(f".{uuid4().hex}.tmp")
    temporary.write_text(json.dumps(value, default=json_default, ensure_ascii=False, indent=2), encoding="utf-8")
    temporary.replace(path)
    return str(path)


@contextmanager
def database():
    client = MongoClient(os.getenv("MONGO_URI", "mongodb://127.0.0.1:27017"), serverSelectionTimeoutMS=5000)
    try:
        client.admin.command("ping")
        yield client[os.getenv("MONGO_DATABASE", "bigdata_midterm")]
    finally:
        client.close()


@contextmanager
def maintenance_lock(db):
    """No lease expiry: long imports cannot outlive a lock and overlap another job."""
    token = uuid4().hex
    try:
        db[STATE].insert_one({"_id": "maintenance_lock", "token": token, "started_at": now()})
    except DuplicateKeyError as exc:
        raise RuntimeError("Another ingestion/refresh is running; inspect phase2_state maintenance_lock") from exc
    try:
        yield
    finally:
        db[STATE].delete_one({"_id": "maintenance_lock", "token": token})
