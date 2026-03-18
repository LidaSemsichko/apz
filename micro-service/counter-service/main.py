import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from sqlalchemy import Column, Float, String, create_engine, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, declarative_base

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+psycopg2://postgres:postgres@postgres:5432/appdb",
)

engine = create_engine(DATABASE_URL, future=True)
Base = declarative_base()


class Account(Base):
    __tablename__ = "accounts"

    user_id = Column(String, primary_key=True)
    balance = Column(Float, nullable=False, default=0.0)


class AppliedTransaction(Base):
    __tablename__ = "applied_transactions"

    transaction_id = Column(String, primary_key=True)
    user_id = Column(String, nullable=False)
    amount = Column(Float, nullable=False)


class TransactionIn(BaseModel):
    transaction_id: str
    user_id: str
    amount: float


@asynccontextmanager
async def lifespan(app: FastAPI):
    Base.metadata.create_all(bind=engine)
    yield


app = FastAPI(lifespan=lifespan)


@app.get("/health")
def health():
    return {"status": "ok", "service": "counter-service"}


@app.post("/apply")
def apply_transaction(tx: TransactionIn):
    with Session(engine) as session:
        existing = session.get(AppliedTransaction, tx.transaction_id)
        if existing is not None:
            account = session.get(Account, tx.user_id)
            balance = account.balance if account else 0.0
            return {
                "status": "duplicate",
                "transaction_id": tx.transaction_id,
                "balance": balance,
            }

        account = session.execute(
            select(Account)
            .where(Account.user_id == tx.user_id)
            .with_for_update()
        ).scalar_one_or_none()

        if account is None:
            account = Account(user_id=tx.user_id, balance=0.0)
            session.add(account)
            session.flush()

        account.balance += tx.amount
        session.add(
            AppliedTransaction(
                transaction_id=tx.transaction_id,
                user_id=tx.user_id,
                amount=tx.amount,
            )
        )
        session.commit()

        return {
            "status": "applied",
            "transaction_id": tx.transaction_id,
            "balance": account.balance,
        }


@app.get("/account/{user_id}")
def get_account(user_id: str):
    with Session(engine) as session:
        account = session.get(Account, user_id)
        txs = session.execute(
            select(AppliedTransaction).where(AppliedTransaction.user_id == user_id)
        ).scalars().all()

        return {
            "user_id": user_id,
            "balance": account.balance if account else 0.0,
            "transactions": [
                {
                    "transaction_id": tx.transaction_id,
                    "amount": tx.amount,
                }
                for tx in txs
            ],
        }


@app.get("/accounts")
def get_accounts():
    with Session(engine) as session:
        rows = session.execute(select(Account)).scalars().all()
        return {
            "balances": {row.user_id: row.balance for row in rows}
        }


@app.post("/reset")
def reset_system():
    with Session(engine) as session:
        session.query(AppliedTransaction).delete()
        session.query(Account).delete()
        session.commit()
    return {"status": "reset_all"}