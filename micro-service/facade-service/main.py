import asyncio
import json
import os
import time
from contextlib import asynccontextmanager

import hazelcast
import httpx
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

SERVICE_NAME = os.getenv("SERVICE_NAME", "facade-service")

LOGGING_URL = os.getenv("LOGGING_URL", "http://logging-service:8000")
COUNTER_URL = os.getenv("COUNTER_URL", "http://counter-service:8000")

HAZELCAST_MEMBERS = os.getenv(
    "HAZELCAST_MEMBERS",
    "hazelcast-1:5701,hazelcast-2:5701,hazelcast-3:5701",
).split(",")

QUEUE_NAME = os.getenv("QUEUE_NAME", "transaction-queue")

app = FastAPI()

hz_client = None
hz_queue = None

metrics = {
    "logging": {"calls": 0, "total_s": 0.0, "avg_ms": 0.0},
    "counter_get": {"calls": 0, "total_s": 0.0, "avg_ms": 0.0},
    "queue": {"calls": 0, "total_s": 0.0, "avg_ms": 0.0},
}


class TransactionIn(BaseModel):
    user_id: str
    amount: float
    transaction_id: str


def update_metric(name: str, elapsed_s: float):
    metrics[name]["calls"] += 1
    metrics[name]["total_s"] += elapsed_s
    metrics[name]["avg_ms"] = metrics[name]["total_s"] / metrics[name]["calls"] * 1000


async def call_logging(payload: dict):
    last_error = None

    async with httpx.AsyncClient(timeout=5.0) as client:
        for _ in range(3):
            try:
                start = time.perf_counter()
                resp = await client.post(f"{LOGGING_URL}/log", json=payload)
                elapsed = time.perf_counter() - start
                update_metric("logging", elapsed)

                resp.raise_for_status()
                return resp.json()

            except Exception as e:
                last_error = e
                await asyncio.sleep(0.2)

    raise HTTPException(
        status_code=503,
        detail=f"logging-service unavailable: {last_error}",
    )


async def enqueue_transaction(payload: dict):
    try:
        start = time.perf_counter()
        await asyncio.to_thread(hz_queue.put, json.dumps(payload))
        elapsed = time.perf_counter() - start
        update_metric("queue", elapsed)
        print(f"[{SERVICE_NAME}] Queued tx={payload['transaction_id']}")
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Queue unavailable: {e}")


async def call_counter_get(path: str):
    last_error = None

    async with httpx.AsyncClient(timeout=5.0) as client:
        for _ in range(3):
            try:
                start = time.perf_counter()
                resp = await client.get(f"{COUNTER_URL}{path}")
                elapsed = time.perf_counter() - start
                update_metric("counter_get", elapsed)

                resp.raise_for_status()
                return resp.json()

            except Exception as e:
                last_error = e
                await asyncio.sleep(0.2)

    raise HTTPException(
        status_code=503,
        detail=f"counter-service unavailable: {last_error}",
    )


@asynccontextmanager
async def lifespan(app: FastAPI):
    global hz_client, hz_queue

    hz_client = hazelcast.HazelcastClient(cluster_members=HAZELCAST_MEMBERS)
    hz_queue = hz_client.get_queue(QUEUE_NAME).blocking()

    print(f"[{SERVICE_NAME}] Connected to Hazelcast: {HAZELCAST_MEMBERS}")
    print(f"[{SERVICE_NAME}] Connected to queue: {QUEUE_NAME}")
    print(f"[{SERVICE_NAME}] LOGGING_URL={LOGGING_URL}")
    print(f"[{SERVICE_NAME}] COUNTER_URL={COUNTER_URL}")

    yield

    hz_client.shutdown()


app = FastAPI(lifespan=lifespan)


@app.get("/health")
def health():
    return {"status": "ok", "service": SERVICE_NAME}


@app.post("/metrics/reset")
def reset_metrics():
    metrics["logging"] = {"calls": 0, "total_s": 0.0, "avg_ms": 0.0}
    metrics["counter_get"] = {"calls": 0, "total_s": 0.0, "avg_ms": 0.0}
    metrics["queue"] = {"calls": 0, "total_s": 0.0, "avg_ms": 0.0}
    return {"status": "metrics_reset"}


@app.get("/metrics")
def get_metrics():
    return metrics


@app.post("/system/reset")
async def reset_system():
    async with httpx.AsyncClient(timeout=10.0) as client:
        resp = await client.post(f"{COUNTER_URL}/reset")
        resp.raise_for_status()

    await asyncio.to_thread(hz_queue.clear)

    return {"status": "reset_all"}


@app.post("/transaction")
async def create_transaction(tx: TransactionIn):
    log_payload = {
        "transaction_id": tx.transaction_id,
        "user_id": tx.user_id,
        "amount": tx.amount,
    }

    queue_payload = {
        "transaction_id": tx.transaction_id,
        "user_id": tx.user_id,
        "amount": tx.amount,
    }

    log_result = await call_logging(log_payload)
    await enqueue_transaction(queue_payload)

    return {
        "transaction_id": tx.transaction_id,
        "logging": log_result,
        "counter_status": "queued",
    }


@app.get("/accounts")
async def accounts():
    return await call_counter_get("/accounts")


@app.get("/user/{user_id}")
async def user_account(user_id: str):
    return await call_counter_get(f"/account/{user_id}")