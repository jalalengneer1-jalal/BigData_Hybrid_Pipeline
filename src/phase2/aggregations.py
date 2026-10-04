from .common import FACTS, DIRTY

REPORTS = {
    "sales_by_city": "Order totals by city and currency (includes delivery)",
    "top_products": "Product revenue by SKU/name fallback and currency (excludes delivery)",
    "top_customers": "Customer spending by currency",
    "sales_by_day": "Daily order totals by currency (includes delivery)",
    "orders_by_status": "Order counts and totals by status and currency",
}


def pipeline(name, limit=100, match=None):
    if name not in REPORTS:
        raise KeyError(name)
    if not 1 <= limit <= 1000:
        raise ValueError("limit must be between 1 and 1000")
    stages = [{"$match": match}] if match else []
    if name == "top_products":
        stages += [{"$unwind": "$items"}, {"$group": {
            "_id": {"product_key": "$items.product_key", "currency": "$currency"},
            "quantity": {"$sum": "$items.qty"}, "revenue": {"$sum": "$items.total"},
        }}]
    else:
        field = {"sales_by_city": "city", "top_customers": "customer_id", "sales_by_day": "day", "orders_by_status": "status"}[name]
        stages += [{"$group": {"_id": {field: f"${field}", "currency": "$currency"},
                               "orders": {"$sum": 1}, "revenue": {"$sum": "$total_amount"}}}]
    sort = {"_id.day": 1, "_id.currency": 1} if name == "sales_by_day" else {"revenue": -1, "_id": 1}
    return stages + [{"$sort": sort}, {"$limit": limit}]


def run_report(db, name, limit=100):
    if name not in REPORTS:
        raise KeyError(name)
    if db[DIRTY].find_one({}):
        raise ValueError("A refresh was interrupted; run refresh-mv before reading reports")
    state = db.phase2_state.find_one({"_id": "facts_checkpoint"})
    if not state:
        raise ValueError("Run refresh-mv first to initialize analytics facts")
    rows = list(db[FACTS].aggregate(pipeline(name, limit), allowDiskUse=True, maxTimeMS=120000))
    return {"name": name, "description": REPORTS[name], "as_of": state["watermark"], "limit": limit, "returned": len(rows), "rows": rows}
