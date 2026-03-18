import os
import random
import time

import httpx
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

LOGGING_URLS = [
    url.strip()
    for url in os.getenv("LOGGING_URLS", "").split(",")
    if url.strip()
]
COUNTER_URL = os.getenv("COUNTER_URL", "http://counter-service:8000")

app = FastAPI()

metrics = {
    "logging": {"calls": 0, "total_s": 0.0, "avg_ms": 0.0},
    "counter": {"calls": 0, "total_s": 0.0, "avg_ms": 0.0},
}


class TransactionIn(BaseModel):
    user_Id: str
    amount: float
    transaction_id: str


def update_metric(name: str, elapsed_s: float):
    metrics[name]["calls"] += 1
    metrics[name]["total_s"] += elapsed_s
    metrics[name]["avg_ms"] = (
        metrics[name]["total_s"] / metrics[name]["calls"] * 1000
    )


async def call_logging(method: str, path: str, payload=None):
    urls = LOGGING_URLS[:]
    random.shuffle(urls)

    last_error = None
    async with httpx.AsyncClient(timeout=5.0) as client:
        for base_url in urls:
            try:
                start = time.perf_counter()
                if method == "POST":
                    resp = await client.post(f"{base_url}{path}", json=payload)
                else:
                    resp = await client.get(f"{base_url}{path}")
                elapsed = time.perf_counter() - start
                update_metric("logging", elapsed)
                resp.raise_for_status()
                return resp.json()
            except Exception as e:
                last_error = e
                continue

    raise HTTPException(
        status_code=503,
        detail=f"All logging-service instances failed: {last_error}",
    )


async def call_counter(method: str, path: str, payload=None):
    async with httpx.AsyncClient(timeout=5.0) as client:
        start = time.perf_counter()
        if method == "POST":
            resp = await client.post(f"{COUNTER_URL}{path}", json=payload)
        else:
            resp = await client.get(f"{COUNTER_URL}{path}")
        elapsed = time.perf_counter() - start
        update_metric("counter", elapsed)
        resp.raise_for_status()
        return resp.json()


@app.get("/health")
def health():
    return {"status": "ok", "service": "facade-service"}


@app.post("/metrics/reset")
def reset_metrics():
    metrics["logging"] = {"calls": 0, "total_s": 0.0, "avg_ms": 0.0}
    metrics["counter"] = {"calls": 0, "total_s": 0.0, "avg_ms": 0.0}
    return {"status": "metrics_reset"}


@app.get("/metrics")
def get_metrics():
    return metrics


@app.post("/system/reset")
async def reset_system():
    async with httpx.AsyncClient(timeout=10.0) as client:
        resp = await client.post(f"{COUNTER_URL}/reset")
        resp.raise_for_status()
        return {"status": "reset_all"}


@app.post("/transaction")
async def create_transaction(tx: TransactionIn):
    log_payload = {
        "transaction_id": tx.transaction_id,
        "user_id": tx.user_Id,
        "amount": tx.amount,
    }
    counter_payload = {
        "transaction_id": tx.transaction_id,
        "user_id": tx.user_Id,
        "amount": tx.amount,
    }

    log_result = await call_logging("POST", "/log", log_payload)
    counter_result = await call_counter("POST", "/apply", counter_payload)

    return {
        "transaction_id": tx.transaction_id,
        "logging": log_result,
        "balance": counter_result["balance"],
    }


@app.get("/accounts")
async def accounts():
    return await call_counter("GET", "/accounts")


@app.get("/user/{user_id}")
async def user_account(user_id: str):
    balance_data = await call_counter("GET", f"/account/{user_id}")

    txs = []
    return {
        "balance": balance_data["balance"],
        "transactions": txs,
    }