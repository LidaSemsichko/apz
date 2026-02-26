import os
import time
import uuid
import asyncio
from typing import Any, Dict, List, Optional, Tuple

import httpx
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

LOGGING_URL = os.getenv("LOGGING_URL", "http://localhost:8001")
COUNTER_URL = os.getenv("COUNTER_URL", "http://localhost:8002")

app = FastAPI(title="facade-service")

# ---- Metrics (accumulate timing separately) ----
class TimingStats:
    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self.logging_calls = 0
        self.logging_total_s = 0.0
        self.counter_calls = 0
        self.counter_total_s = 0.0

    async def add_logging(self, elapsed_s: float) -> None:
        async with self._lock:
            self.logging_calls += 1
            self.logging_total_s += elapsed_s

    async def add_counter(self, elapsed_s: float) -> None:
        async with self._lock:
            self.counter_calls += 1
            self.counter_total_s += elapsed_s

    async def reset(self) -> None:
        async with self._lock:
            self.logging_calls = 0
            self.logging_total_s = 0.0
            self.counter_calls = 0
            self.counter_total_s = 0.0

    async def snapshot(self) -> Dict[str, Any]:
        async with self._lock:
            def avg_ms(total_s: float, calls: int) -> float:
                return (total_s / calls * 1000.0) if calls else 0.0

            return {
                "logging": {
                    "calls": self.logging_calls,
                    "total_s": round(self.logging_total_s, 6),
                    "avg_ms": round(avg_ms(self.logging_total_s, self.logging_calls), 3),
                },
                "counter": {
                    "calls": self.counter_calls,
                    "total_s": round(self.counter_total_s, 6),
                    "avg_ms": round(avg_ms(self.counter_total_s, self.counter_calls), 3),
                },
            }

stats = TimingStats()

# ---- Models ----
class ClientTransaction(BaseModel):
    user_Id: str = Field(..., min_length=1)
    amount: float
    transaction_id: str | None = None

class InternalTransaction(BaseModel):
    transaction_id: str
    timestamp: float
    user_Id: str
    amount: float

class PostResponse(BaseModel):
    transaction_id: str
    balance: float

class UserView(BaseModel):
    balance: float
    transactions: List[Dict[str, Any]]

class AccountsView(BaseModel):
    balances: Dict[str, float]


# ---- HTTP client ----
_client: Optional[httpx.AsyncClient] = None

@app.on_event("startup")
async def startup() -> None:
    global _client
    # Long-lived client is faster and avoids TCP setup cost each request.
    _client = httpx.AsyncClient(timeout=10.0)

@app.on_event("shutdown")
async def shutdown() -> None:
    global _client
    if _client is not None:
        await _client.aclose()
        _client = None

def _must_client() -> httpx.AsyncClient:
    if _client is None:
        raise RuntimeError("HTTP client not initialized")
    return _client


async def _timed_request(method: str, url: str, json_body: Optional[Dict[str, Any]] = None) -> Tuple[float, httpx.Response]:
    c = _must_client()
    start = time.perf_counter()
    resp = await c.request(method, url, json=json_body)
    elapsed = time.perf_counter() - start
    return elapsed, resp


@app.get("/health")
async def health() -> Dict[str, str]:
    return {"status": "ok"}


@app.post("/transaction", response_model=PostResponse)
async def post_transaction(tx: ClientTransaction) -> PostResponse:
    """
    Flow:
    1) generate transaction_id + timestamp
    2) POST to logging-service (/log)
    3) POST to counter-service (/apply) and get updated balance
    4) wait BOTH responses; return to client {transaction_id, balance}
    """
    transaction_id = tx.transaction_id or str(uuid.uuid4())
    ts = time.time()
    msg = InternalTransaction(
        transaction_id=transaction_id,
        timestamp=ts,
        user_Id=tx.user_Id,
        amount=tx.amount,
    ).model_dump()

    # Do both calls concurrently (but we still wait for both).
    async def call_logging() -> None:
        elapsed, resp = await _timed_request("POST", f"{LOGGING_URL}/log", json_body=msg)
        await stats.add_logging(elapsed)
        if resp.status_code >= 400:
            raise HTTPException(status_code=502, detail=f"logging-service error: {resp.status_code} {resp.text}")

    async def call_counter() -> float:
        elapsed, resp = await _timed_request("POST", f"{COUNTER_URL}/apply", json_body=msg)
        await stats.add_counter(elapsed)
        if resp.status_code >= 400:
            raise HTTPException(status_code=502, detail=f"counter-service error: {resp.status_code} {resp.text}")
        data = resp.json()
        if "balance" not in data:
            raise HTTPException(status_code=502, detail="counter-service response missing 'balance'")
        return float(data["balance"])

    try:
        logging_task = asyncio.create_task(call_logging())
        counter_task = asyncio.create_task(call_counter())
        balance = await counter_task
        await logging_task
        return PostResponse(transaction_id=transaction_id, balance=balance)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"facade internal error: {type(e).__name__}: {e}")


@app.get("/user/{user_Id}", response_model=UserView)
async def get_user(user_Id: str) -> UserView:
    """
    Must return:
    - balance from counter-service
    - all transactions for this user from logging-service
    """
    if not user_Id:
        raise HTTPException(status_code=400, detail="user_Id required")

    # counter
    elapsed_c, resp_c = await _timed_request("GET", f"{COUNTER_URL}/balance/{user_Id}")
    await stats.add_counter(elapsed_c)
    if resp_c.status_code >= 400:
        raise HTTPException(status_code=502, detail=f"counter-service error: {resp_c.status_code} {resp_c.text}")
    bal = float(resp_c.json().get("balance", 0.0))

    # logging
    elapsed_l, resp_l = await _timed_request("GET", f"{LOGGING_URL}/logs/user/{user_Id}")
    await stats.add_logging(elapsed_l)
    if resp_l.status_code >= 400:
        raise HTTPException(status_code=502, detail=f"logging-service error: {resp_l.status_code} {resp_l.text}")
    txs = resp_l.json().get("transactions", [])
    if not isinstance(txs, list):
        txs = []

    return UserView(balance=bal, transactions=txs)


@app.get("/accounts", response_model=AccountsView)
async def get_accounts() -> AccountsView:
    """
    Must return balances of all accounts (from counter-service).
    """
    elapsed, resp = await _timed_request("GET", f"{COUNTER_URL}/balances")
    await stats.add_counter(elapsed)
    if resp.status_code >= 400:
        raise HTTPException(status_code=502, detail=f"counter-service error: {resp.status_code} {resp.text}")
    data = resp.json()
    balances = data.get("balances", {})
    # normalize to float
    out = {str(k): float(v) for k, v in balances.items()}
    return AccountsView(balances=out)


@app.get("/metrics")
async def get_metrics() -> Dict[str, Any]:
    """
    Read accumulated timing stats for remote calls.
    """
    return await stats.snapshot()


@app.post("/metrics/reset")
async def reset_metrics() -> Dict[str, str]:
    """
    Reset accumulated timing stats.
    """
    await stats.reset()
    return {"status": "reset"}



@app.post("/system/reset")
async def system_reset() -> Dict[str, Any]:
    # reset metrics
    await stats.reset()

    # reset logging + counter
    elapsed_l, resp_l = await _timed_request("POST", f"{LOGGING_URL}/reset")
    await stats.add_logging(elapsed_l)
    if resp_l.status_code >= 400:
        raise HTTPException(status_code=502, detail=f"logging reset failed: {resp_l.status_code} {resp_l.text}")

    elapsed_c, resp_c = await _timed_request("POST", f"{COUNTER_URL}/reset")
    await stats.add_counter(elapsed_c)
    if resp_c.status_code >= 400:
        raise HTTPException(status_code=502, detail=f"counter reset failed: {resp_c.status_code} {resp_c.text}")

    return {"status": "reset_all"}
