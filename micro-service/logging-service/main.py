import os
import time
from contextlib import asynccontextmanager
import hazelcast
import httpx
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

SERVICE_NAME = os.getenv("SERVICE_NAME", "logging-service")
SERVICE_URL = os.getenv("SERVICE_URL", "http://logging-service:8000")
CONFIG_SERVER_URL = os.getenv("CONFIG_SERVER_URL", "http://config-service:8000")

HAZELCAST_MEMBERS = os.getenv(
    "HAZELCAST_MEMBERS",
    "hazelcast-1:5701,hazelcast-2:5701,hazelcast-3:5701",
).split(",")

HAZELCAST_MAP_NAME = os.getenv("HAZELCAST_MAP_NAME", "logs")

hz_client = None
hz_map = None


class LogMessage(BaseModel):
    transaction_id: str
    user_id: str
    amount: float
    timestamp: float | None = None


async def register_on_config_server():
    payload = {
        "service_name": "logging-service",
        "service_url": SERVICE_URL,
    }

    async with httpx.AsyncClient(timeout=10.0) as client:
        resp = await client.post(f"{CONFIG_SERVER_URL}/register", json=payload)
        resp.raise_for_status()
        print(f"[{SERVICE_NAME}] Registered on config-server: {payload}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    global hz_client, hz_map

    hz_client = hazelcast.HazelcastClient(cluster_members=HAZELCAST_MEMBERS)
    hz_map = hz_client.get_map(HAZELCAST_MAP_NAME).blocking()

    print(f"[{SERVICE_NAME}] Connected to Hazelcast: {HAZELCAST_MEMBERS}")

    await register_on_config_server()

    yield

    hz_client.shutdown()


app = FastAPI(lifespan=lifespan)


@app.get("/health")
def health():
    return {"status": "ok", "service": SERVICE_NAME}


@app.post("/log")
def write_log(msg: LogMessage):
    if msg.timestamp is None:
        msg.timestamp = time.time()

    existing = hz_map.get(msg.transaction_id)
    if existing is not None:
        print(f"[{SERVICE_NAME}] Duplicate log ignored: {msg.transaction_id}")
        return {
            "status": "duplicate",
            "service": SERVICE_NAME,
            "transaction_id": msg.transaction_id,
        }

    payload = msg.model_dump()
    hz_map.set(msg.transaction_id, payload)

    print(
        f"[{SERVICE_NAME}] Stored tx={msg.transaction_id} "
        f"user={msg.user_id} amount={msg.amount}"
    )

    return {
        "status": "stored",
        "service": SERVICE_NAME,
        "transaction_id": msg.transaction_id,
    }


@app.get("/log/{transaction_id}")
def read_log(transaction_id: str):
    value = hz_map.get(transaction_id)
    if value is None:
        raise HTTPException(status_code=404, detail="Transaction not found")
    return value