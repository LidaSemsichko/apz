import json
import os
import random
import time
from contextlib import asynccontextmanager

import hazelcast
import httpx
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

SERVICE_NAME = os.getenv("SERVICE_NAME", "facade-service")
SERVICE_URL = os.getenv("SERVICE_URL", "http://facade-service:8000")
CONFIG_SERVER_URL = os.getenv("CONFIG_SERVER_URL", "http://config-service:8000")

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


async def register_on_config_server():
    payload = {
        "service_name": "facade-service",
        "service_url": SERVICE_URL,
    }

    async with httpx.AsyncClient(timeout=10.0) as client:
        resp = await client.post(f"{CONFIG_SERVER_URL}/register", json=payload)
        resp.raise_for_status()
        print(f"[{SERVICE_NAME}] Registered on config-server: {payload}")


async def get_service_instances(service_name: str) -> list[str]:
    async with httpx.AsyncClient(timeout=5.0) as client:
        resp = await client.get(f"{CONFIG_SERVER_URL}/services/{service_name}")
        resp.raise_for_status()
        data = resp.json()
        return data.get("instances", [])


async def choose_service_instance(service_name: str) -> str:
    instances = await get_service_instances(service_name)

    if not instances:
        raise HTTPException(
            status_code=503,
            detail=f"No instances available for {service_name}",
        )

    return random.choice(instances)


async def call_logging(payload: dict):
    instances = await get_service_instances("logging-service")
    if not instances:
        raise HTTPException(status_code=503, detail="No logging-service instances")

    shuffled = instances[:]
    random.shuffle(shuffled)

    last_error = None

    async with httpx.AsyncClient(timeout=5.0) as client:
        for base_url in shuffled:
            try:
                start = time.perf_counter()
                resp = await client.post(f"{base_url}/log", json=payload)
                elapsed = time.perf_counter() - start
                update_metric("logging", elapsed)

                resp.raise_for_status()
                print(
                    f"[{SERVICE_NAME}] Logged tx={payload['transaction_id']} via {base_url}"
                )
                return resp.json()

            except Exception as e:
                last_error = e
                print(f"[{SERVICE_NAME}] logging-service failed: {base_url} -> {e}")
                continue

    raise HTTPException(
        status_code=503,
        detail=f"All logging-service instances failed: {last_error}",
    )


async def enqueue_transaction(payload: dict):
    try:
        start = time.perf_counter()
        await asyncio_to_thread_put(json.dumps(payload))
        elapsed = time.perf_counter() - start
        update_metric("queue", elapsed)
        print(f"[{SERVICE_NAME}] Queued tx={payload['transaction_id']}")
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Queue unavailable: {e}")


async def asyncio_to_thread_put(item: str):
    import asyncio
    await asyncio.to_thread(hz_queue.put, item)


async def call_counter_get(path: str):
    instances = await get_service_instances("counter-service")
    if not instances:
        raise HTTPException(status_code=503, detail="No counter-service instances")

    shuffled = instances[:]
    random.shuffle(shuffled)

    last_error = None

    async with httpx.AsyncClient(timeout=5.0) as client:
        for base_url in shuffled:
            try:
                start = time.perf_counter()
                resp = await client.get(f"{base_url}{path}")
                elapsed = time.perf_counter() - start
                update_metric("counter_get", elapsed)

                resp.raise_for_status()
                return resp.json()

            except Exception as e:
                last_error = e
                print(f"[{SERVICE_NAME}] counter-service failed: {base_url} -> {e}")
                continue

    raise HTTPException(
        status_code=503,
        detail=f"All counter-service instances failed: {last_error}",
    )


@asynccontextmanager
async def lifespan(app: FastAPI):
    global hz_client, hz_queue

    hz_client = hazelcast.HazelcastClient(cluster_members=HAZELCAST_MEMBERS)
    hz_queue = hz_client.get_queue(QUEUE_NAME).blocking()

    print(f"[{SERVICE_NAME}] Connected to Hazelcast: {HAZELCAST_MEMBERS}")
    print(f"[{SERVICE_NAME}] Connected to queue: {QUEUE_NAME}")

    await register_on_config_server()

    yield

    hz_client.shutdown()


app = FastAPI(lifespan=lifespan)


@app.get("/health")
def health():
    return {"status": "ok", "service": "facade-service"}


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
    instances = await get_service_instances("counter-service")
    if not instances:
        raise HTTPException(status_code=503, detail="No counter-service instances")

    base_url = random.choice(instances)

    async with httpx.AsyncClient(timeout=10.0) as client:
        resp = await client.post(f"{base_url}/reset")
        resp.raise_for_status()
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