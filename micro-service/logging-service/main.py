import asyncio
from typing import Any, Dict, List

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

app = FastAPI(title="logging-service")

# In-memory storage: transaction_id -> message
_storage: Dict[str, Dict[str, Any]] = {}
_lock = asyncio.Lock()

class InternalTransaction(BaseModel):
    transaction_id: str = Field(..., min_length=1)
    timestamp: float
    user_Id: str = Field(..., min_length=1)
    amount: float

@app.get("/health")
async def health() -> Dict[str, str]:
    return {"status": "ok"}

@app.post("/log")
async def log_transaction(tx: InternalTransaction) -> Dict[str, str]:
    async with _lock:
        _storage[tx.transaction_id] = tx.model_dump()
    # optional debug print:
    # print("LOG:", tx.model_dump())
    return {"status": "stored"}

@app.get("/logs")
async def get_all_logs() -> Dict[str, List[Dict[str, Any]]]:
    async with _lock:
        return {"transactions": list(_storage.values())}

@app.get("/logs/user/{user_Id}")
async def get_user_logs(user_Id: str) -> Dict[str, List[Dict[str, Any]]]:
    if not user_Id:
        raise HTTPException(status_code=400, detail="user_Id required")
    async with _lock:
        txs = [v for v in _storage.values() if v.get("user_Id") == user_Id]
    return {"transactions": txs}



@app.post("/reset")
async def reset_state() -> Dict[str, str]:
    async with _lock:
        _storage.clear()
    return {"status": "reset"}
