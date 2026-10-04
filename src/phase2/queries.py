from datetime import date

from .common import save_report, now

COLLECTION = "orders_validated"
INDEXES = [
    ("p2_customer_date", [("customer_id", 1), ("order_date", 1)], "Customer equality followed by date range/sort"),
    ("p2_city_date", [("city", 1), ("order_date", 1)], "City equality followed by date range/sort"),
    ("p2_status_date", [("status", 1), ("order_date", 1)], "Status equality followed by date range/sort"),
    ("p2_updated", [("last_updated_at", 1)], "Incremental materialized-view synchronization"),
    ("p2_order_date", [("order_date", 1)], "Date-only lookup and chronological sort"),
]
QUERIES = {
    "customer_orders": "Orders for one customer, optionally within a date interval",
    "city_orders": "Orders for one city, optionally within a date interval",
    "status_orders": "Orders with a given status, optionally within a date interval",
    "date_orders": "Orders in a required date interval [start, end)",
    "quality_orders": "Orders by valid/corrected quality classification",
}


def create_indexes(db):
    return [{"name": db[COLLECTION].create_index(keys, name=name), "keys": keys, "reason": reason}
            for name, keys, reason in INDEXES]


def query_spec(name, value=None, start=None, end=None):
    if name not in QUERIES:
        raise KeyError(name)
    fields = {"customer_orders": "customer_id", "city_orders": "city", "status_orders": "status", "quality_orders": "quality_status"}
    match = {}
    if name in fields:
        if not value:
            raise ValueError("value is required for this query")
        if name == "quality_orders" and value not in {"valid", "corrected"}:
            raise ValueError("quality value must be valid or corrected")
        match[fields[name]] = value
    for boundary in (start, end):
        if boundary:
            date.fromisoformat(boundary)
    if start and end and start >= end:
        raise ValueError("start must be before end")
    if name == "date_orders" and not (start and end):
        raise ValueError("date_orders requires start and end (YYYY-MM-DD)")
    if start or end:
        match["order_date"] = {}
        if start:
            match["order_date"]["$gte"] = start
        if end:
            match["order_date"]["$lt"] = end
    return match


def run_query(db, name, value=None, start=None, end=None, limit=100):
    if not 1 <= limit <= 1000:
        raise ValueError("limit must be between 1 and 1000")
    match = query_spec(name, value, start, end)
    rows = list(db[COLLECTION].find(match, {"_id": 0, "corrections": 0}).sort("order_date", 1).limit(limit).max_time_ms(60000))
    return {"name": name, "filter": match, "limit": limit, "returned": len(rows), "rows": rows}


def explain_comparison(db):
    """Use an explicit COLLSCAN baseline without dropping midterm indexes."""
    example = db[COLLECTION].find_one({}, {"customer_id": 1, "city": 1, "status": 1})
    if not example:
        raise ValueError("Ingest data first: Explain requires actual validated orders")
    comparisons = []
    for query, field, index in [("customer_orders", "customer_id", "p2_customer_date"), ("city_orders", "city", "p2_city_date"), ("status_orders", "status", "p2_status_date")]:
        if not example.get(field):
            raise ValueError(f"An actual {field} value is required for Explain")
        find = {"find": COLLECTION, "filter": {field: example[field]}, "sort": {"order_date": 1}, "limit": 100}
        before = db.command("explain", {**find, "hint": {"$natural": 1}}, verbosity="executionStats")
        create_indexes(db)
        after = db.command("explain", {**find, "hint": index}, verbosity="executionStats")
        comparisons.append({"name": query, "index": index, "value": example[field],
                            "before": before, "after": after,
                            "reason": next(reason for n, _, reason in INDEXES if n == index)})
    result = {"generated_at": now(), "baseline": "Forced natural collection scan; existing indexes are preserved. After uses named compound index.", "comparisons": comparisons}
    save_report("explain", result)
    return result
