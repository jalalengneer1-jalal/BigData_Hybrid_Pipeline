"""Real MongoDB integration checks. Only isolated phase2_test_* databases are used."""
import csv
import json
import os
import socket
import threading
import time
import urllib.error
import urllib.request
from decimal import Decimal
from uuid import uuid4

import pytest
from pymongo import MongoClient
from pymongo.collection import Collection

from src.phase2 import aggregations, queries, views, jobs
from src.phase2.common import DAILY, PRODUCTS, FACTS, DIRTY, STATE, LOGS, now, maintenance_lock
from src.quality_rules import classify_record
from tests.test_cleaning_rules import base_record


@pytest.fixture
def db(monkeypatch):
    uri = os.getenv("PHASE2_TEST_MONGO_URI")
    if not uri:
        pytest.skip("Set PHASE2_TEST_MONGO_URI to run real MongoDB integration checks")
    client = MongoClient(uri, serverSelectionTimeoutMS=5000)
    client.admin.command("ping")
    name = "phase2_test_" + uuid4().hex
    monkeypatch.setenv("MONGO_URI", uri)
    monkeypatch.setenv("MONGO_DATABASE", name)
    database = client[name]
    try:
        yield database
    finally:
        assert name.startswith("phase2_test_")
        client.drop_database(name)
        client.close()


def order(key="one", day="2025-01-31", currency="YER", sku="SKU-1", total="12000", qty=2, item_total=10000):
    record = classify_record(base_record())["cleaned_record"]
    record.update(order_id=key, order_date=day + "T12:00:00", currency=currency, total_amount=total,
                  items_json=json.dumps([{"sku": sku, "qty": qty, "total": item_total}]), last_updated_at=now(), quality_status="valid")
    return record


def test_incremental_update_and_currency_separation(db):
    db.orders_validated.insert_many([order(), order("two", currency="USD", total="12.25", item_total="10.25")])
    result = views.refresh(db, batch_size=1)
    assert result["changed"] == 2
    assert db[DAILY].count_documents({}) == 2
    assert db[DAILY].find_one({"_id.currency": "USD"})["revenue"].to_decimal() == Decimal("12.25")
    assert views.refresh(db)["changed"] == 0
    db.orders_validated.replace_one({"order_id": "one"}, order(day="2025-02-01", sku="SKU-2", total="15000", item_total=13000))
    result = views.refresh(db)
    assert result["changed"] == 1
    assert db[DAILY].find_one({"_id.day": "2025-01-31", "_id.currency": "YER"}) is None
    assert db[PRODUCTS].find_one({"_id.product_key": "sku:SKU-1", "_id.currency": "YER"}) is None
    assert db[PRODUCTS].find_one({"_id.product_key": "sku:SKU-2"})["revenue"].to_decimal() == 13000
    # Updating a group that still has another order keeps that order's contribution.
    db.orders_validated.insert_one(order("three", day="2025-02-01", sku="SKU-2"))
    views.refresh(db)
    assert db[DAILY].find_one({"_id.currency": "YER"})["orders"] == 2
    assert db[DAILY].find_one({"_id.currency": "YER"})["revenue"].to_decimal() == 27000


def test_reports_queries_and_explain(db, monkeypatch, tmp_path):
    db.orders_validated.insert_many([order(), order("two", total="24000", item_total=20000)])
    views.refresh(db)
    for name in aggregations.REPORTS:
        report = aggregations.run_report(db, name)
        assert report["returned"] >= 1
    totals = aggregations.run_report(db, "sales_by_city")["rows"][0]
    assert totals["orders"] == 2
    assert totals["revenue"].to_decimal() == 36000
    for name, value in [("customer_orders", base_record()["customer_id"]), ("city_orders", base_record()["city"]),
                        ("status_orders", order()["status"]), ("quality_orders", "valid")]:
        assert queries.run_query(db, name, value)["returned"] == 2
    assert queries.run_query(db, "date_orders", start="2025-01-31", end="2025-02-01")["returned"] == 2
    monkeypatch.setattr(queries, "save_report", lambda n, r: None)
    evidence = queries.explain_comparison(db)
    assert len(evidence["comparisons"]) == 3
    for pair in evidence["comparisons"]:
        assert pair["before"]["executionStats"]["nReturned"] == pair["after"]["executionStats"]["nReturned"]
        assert "COLLSCAN" in str(pair["before"]["queryPlanner"])
        assert "IXSCAN" in str(pair["after"]["queryPlanner"])
    assert len(queries.create_indexes(db)) >= 3


def test_crash_recovery_and_lock(db, monkeypatch):
    db.orders_validated.insert_one(order())
    original = Collection.aggregate

    def fail(self, *args, **kwargs):
        if self.name == FACTS:
            raise RuntimeError("simulated interruption after facts write")
        return original(self, *args, **kwargs)

    monkeypatch.setattr(Collection, "aggregate", fail)
    with pytest.raises(RuntimeError):
        views.refresh(db)
    assert db[DIRTY].count_documents({}) > 0
    assert db[STATE].find_one({"_id": "facts_checkpoint"}) is None
    monkeypatch.setattr(Collection, "aggregate", original)
    views.refresh(db)
    assert db[DIRTY].count_documents({}) == 0
    assert db[DAILY].find_one({})["revenue"].to_decimal() == 12000
    with maintenance_lock(db):
        with pytest.raises(RuntimeError, match="Another"):
            views.refresh(db)


def test_job_success_failure_and_scheduler(db, monkeypatch):
    monkeypatch.setattr(jobs, "save_report", lambda *args: None)
    with pytest.raises(ValueError):
        jobs.run_job(db, "daily_reports")
    assert db[LOGS].find_one({"name": "daily_reports"})["status"] == "failed"
    db.orders_validated.insert_one(order())
    assert jobs.run_job(db, "refresh_views")["status"] == "success"
    assert jobs.run_job(db, "daily_reports")["status"] == "success"
    for log in db[LOGS].find({}):
        assert "started_at" in log and "finished_at" in log
    # Exercise timed dispatch twice, with a one-second interval in an isolated DB.
    monkeypatch.setenv("VIEW_REFRESH_SECONDS", "1")
    monkeypatch.setenv("REPORT_INTERVAL_SECONDS", "1")
    stop = threading.Event()
    thread = threading.Thread(target=jobs.scheduler, args=(stop,), daemon=True)
    thread.start()
    deadline = time.monotonic() + 8
    while db[LOGS].count_documents({"name": "refresh_views", "status": "success"}) < 3 and time.monotonic() < deadline:
        time.sleep(0.1)
    stop.set()
    thread.join(5)
    assert not thread.is_alive()
    assert db[LOGS].count_documents({"name": "refresh_views", "status": "success"}) >= 3


def test_api_including_existing_ingestion(db, monkeypatch, tmp_path):
    import uvicorn
    from src.phase2.api import app
    monkeypatch.setattr(jobs, "save_report", lambda *args: None)
    monkeypatch.setattr(queries, "save_report", lambda *args: None)
    # Original ELT emits reports into reports/. Redirect only the child process using
    # a fixture input and preserve/restore those existing reports below.
    from src.phase2.common import ROOT
    report_files = {path: path.read_bytes() for path in (ROOT / "reports").glob("*.json")}
    original_names = set(report_files)
    source = tmp_path / "api_orders.csv"
    record = base_record()
    with source.open("w", encoding="utf-8", newline="") as output:
        writer = csv.DictWriter(output, fieldnames=list(record))
        writer.writeheader()
        writer.writerow(record)
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    server = uvicorn.Server(uvicorn.Config(app, host="127.0.0.1", port=port, log_level="error"))
    thread = threading.Thread(target=server.run, kwargs={"sockets": [sock]}, daemon=True)
    thread.start()

    def request(path, body=None):
        data = None if body is None else json.dumps(body).encode()
        req = urllib.request.Request(f"http://127.0.0.1:{port}" + path, data=data, headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=60) as response:
            payload = response.read()
            return json.loads(payload) if path != "/docs" else payload

    try:
        deadline = time.monotonic() + 10
        while not server.started and time.monotonic() < deadline:
            time.sleep(0.05)
        assert request("/health")["status"] == "ok"
        assert request("/docs")
        assert request("/ingest", {"input_path": str(source)})["status"] == "success"
        assert db.orders_raw.count_documents({}) == 1
        assert db.orders_validated.count_documents({}) == 1
        assert request("/indexes", {})["indexes"]
        assert len(request("/indexes/explain", {})["comparisons"]) == 3
        assert request("/queries")["queries"]
        assert request("/queries/date_orders?start=2025-01-01&end=2025-02-01")["returned"] == 1
        assert request("/refresh-mv", {})["changed"] == 1
        assert request("/aggregations")["aggregations"]
        assert request("/aggregations/top_products")["returned"] == 1
        assert request("/jobs")["jobs"]
        assert request("/jobs/refresh_views/run", {})["status"] == "success"
        assert request("/jobs/daily_reports/run", {})["status"] == "success"
        for path, code in [("/queries/unknown", 404), ("/queries/city_orders", 422), ("/aggregations/unknown", 404)]:
            with pytest.raises(urllib.error.HTTPError) as error:
                request(path)
            assert error.value.code == code
        with pytest.raises(urllib.error.HTTPError) as error:
            request("/ingest", {"input_path": str(tmp_path / "missing.csv")})
        assert error.value.code == 422
    finally:
        server.should_exit = True
        thread.join(10)
        sock.close()
        for path, content in report_files.items():
            path.write_bytes(content)
        for path in (ROOT / "reports").glob("*.json"):
            if path not in original_names:
                path.unlink()


def test_invalid_parameters_and_precise_amounts():
    with pytest.raises(ValueError):
        queries.query_spec("date_orders", start="2025-02-01", end="2025-01-01")
    with pytest.raises(ValueError):
        aggregations.pipeline("top_products", limit=0)
    assert views.money("0.1").to_decimal() + views.money("0.2").to_decimal() == Decimal("0.3")
    assert views.product_key({"name": "  Product   One "}) == "name:product one"
    assert views.product_key({"sku": "name:product one"}) != views.product_key({"name": "Product One"})
