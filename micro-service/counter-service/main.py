import asyncio
from typing import Any, Dict

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

app = FastAPI(title="counter-service")

_balances: Dict[str, float] = {}
_applied: Dict[str, bool] = {}
_lock = asyncio.Lock()

class InternalTransaction(BaseModel):
    transaction_id: str = Field(..., min_length=1)
    timestamp: float
    user_Id: str = Field(..., min_length=1)
    amount: float

@app.get("/health")
async def health() -> Dict[str, str]:
    return {"status": "ok"}

@app.post("/apply")
async def apply_transaction(tx: InternalTransaction) -> Dict[str, Any]:
    async with _lock:
        # idempotency: if already applied, do nothing
        if _applied.get(tx.transaction_id):
            bal = float(_balances.get(tx.user_Id, 0.0))
            return {"user_Id": tx.user_Id, "balance": bal, "dedup": True}

        cur = float(_balances.get(tx.user_Id, 0.0))
        new_bal = cur + float(tx.amount)
        _balances[tx.user_Id] = new_bal
        _applied[tx.transaction_id] = True
        return {"user_Id": tx.user_Id, "balance": new_bal, "dedup": False}

@app.get("/balance/{user_Id}")
async def get_balance(user_Id: str) -> Dict[str, Any]:
    if not user_Id:
        raise HTTPException(status_code=400, detail="user_Id required")
    async with _lock:
        bal = float(_balances.get(user_Id, 0.0))
    return {"user_Id": user_Id, "balance": bal}

@app.get("/balances")
async def get_balances() -> Dict[str, Dict[str, float]]:
    async with _lock:
        return {"balances": dict(_balances)}


@app.post("/reset")
async def reset_state() -> Dict[str, str]:
    async with _lock:
        _balances.clear()
        # якщо ти додавала _applied для ідемпотентності:
        try:
            _applied.clear()
        except NameError:
            pass
    return {"status": "reset"}
