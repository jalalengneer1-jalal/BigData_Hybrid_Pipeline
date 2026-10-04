"""Incremental analytics: bounded batches, durable dirty groups, replace aggregates."""
import hashlib
import json
from datetime import datetime
from decimal import Decimal

from bson import Decimal128
from pymongo import ReplaceOne, UpdateOne

from .aggregations import pipeline
from .common import FACTS, DAILY, PRODUCTS, DIRTY, STATE, now, maintenance_lock
from .queries import create_indexes


def money(value):
    result = Decimal(str(value))
    if not result.is_finite():
        raise ValueError("Non-finite amount in validated data")
    return Decimal128(result)


def normalize(document):
    # The midterm stores normalized items as JSON text and amounts as strings.
    raw_items = document["items_json"]
    items = json.loads(raw_items, parse_float=Decimal) if isinstance(raw_items, str) else raw_items
    day = datetime.fromisoformat(document["order_date"]).date().isoformat()
    fact = {"_id": str(document["order_id"]), "day": day, "currency": document["currency"],
            "city": document.get("city", ""), "status": document.get("status", ""),
            "customer_id": document["customer_id"], "total_amount": money(document["total_amount"]),
            "items": [{"product_key": product_key(item), "qty": money(item["qty"]), "total": money(item["total"])} for item in items]}
    fact["fingerprint"] = hashlib.sha256(json.dumps(fact, default=str, sort_keys=True, ensure_ascii=False).encode()).hexdigest()
    return fact


def product_key(item):
    # Some midterm-validated products have a name but no SKU. Keep explicit
    # namespaces; never pretend a name fallback is a known SKU.
    sku = str(item.get("sku") or "").strip()
    if sku:
        return "sku:" + sku
    name = " ".join(str(item.get("name") or "").split()).casefold()
    return "name:" + name if name else "unknown:unidentified"


def groups(fact):
    if not fact:
        return []
    keys = [{"view": DAILY, "day": fact["day"], "currency": fact["currency"]}]
    keys += [{"view": PRODUCTS, "product_key": key, "currency": fact["currency"]} for key in sorted({item["product_key"] for item in fact["items"]})]
    return keys


def ensure_indexes(db):
    create_indexes(db)
    db[FACTS].create_index([("day", 1), ("currency", 1)], name="p2_fact_day_currency")
    db[FACTS].create_index([("items.product_key", 1), ("currency", 1)], name="p2_fact_product_currency")


def refresh_unlocked(db, batch_size=1000):
    if not 1 <= batch_size <= 5000:
        raise ValueError("batch_size must be between 1 and 5000")
    ensure_indexes(db)
    upper = now()
    checkpoint = db[STATE].find_one({"_id": "facts_checkpoint"})
    match = {"last_updated_at": {"$gte": checkpoint["watermark"], "$lte": upper}} if checkpoint else {
        "$or": [{"last_updated_at": {"$lte": upper}}, {"last_updated_at": {"$exists": False}}]}
    stats = {"scanned": 0, "changed": 0, "dirty_groups_refreshed": 0, "bootstrap": checkpoint is None}

    def flush(batch):
        normalized = [normalize(doc) for doc in batch]
        previous = {doc["_id"]: doc for doc in db[FACTS].find({"_id": {"$in": [f["_id"] for f in normalized]}})}
        writes, unique_marks = [], {}
        for fact in normalized:
            old = previous.get(fact["_id"])
            if old and old["fingerprint"] == fact["fingerprint"]:
                continue
            # Persist OLD and NEW groups before replacing facts, so crashes retain repair work.
            for key in groups(old) + groups(fact):
                unique_marks[tuple(key.items())] = key
            writes.append(ReplaceOne({"_id": fact["_id"]}, fact, upsert=True))
        if unique_marks:
            marks = [UpdateOne({"_id": key}, {"$set": {"pending": True}}, upsert=True) for key in unique_marks.values()]
            db[DIRTY].bulk_write(marks, ordered=True)
            db[FACTS].bulk_write(writes, ordered=True)
        stats["changed"] += len(writes)

    batch = []
    with db.orders_validated.find(match).batch_size(batch_size) as cursor:
        for document in cursor:
            batch.append(document)
            stats["scanned"] += 1
            if len(batch) >= batch_size:
                flush(batch)
                batch = []
        if batch:
            flush(batch)

    # Recompute only affected groups from current facts, never add totals blindly.
    # A moved/removed SKU/day produces an empty result: delete its stale summary.
    with db[DIRTY].find({}).batch_size(batch_size) as cursor:
        for marker in cursor:
            key = marker["_id"]
            view = key["view"]
            if view == DAILY:
                aggregate = pipeline("sales_by_day", match={"day": key["day"], "currency": key["currency"]})[:-2]
                result_key = {"day": key["day"], "currency": key["currency"]}
            else:
                aggregate = [{"$match": {"items.product_key": key["product_key"], "currency": key["currency"]}},
                             {"$unwind": "$items"}, {"$match": {"items.product_key": key["product_key"]}},
                             {"$group": {"_id": {"product_key": "$items.product_key", "currency": "$currency"},
                                          "quantity": {"$sum": "$items.qty"}, "revenue": {"$sum": "$items.total"}}}]
                result_key = {"product_key": key["product_key"], "currency": key["currency"]}
            rows = list(db[FACTS].aggregate(aggregate, allowDiskUse=True, maxTimeMS=120000))
            if rows:
                db[view].replace_one({"_id": result_key}, {**rows[0], "refreshed_at": upper}, upsert=True)
            else:
                db[view].delete_one({"_id": result_key})
            db[DIRTY].delete_one({"_id": key})
            stats["dirty_groups_refreshed"] += 1
    # Only advance once every pending group is safely written. Retry after failure is idempotent.
    db[STATE].replace_one({"_id": "facts_checkpoint"}, {"_id": "facts_checkpoint", "watermark": upper}, upsert=True)
    return {**stats, "watermark": upper, "views": [DAILY, PRODUCTS]}


def refresh(db, batch_size=1000):
    with maintenance_lock(db):
        return refresh_unlocked(db, batch_size)
