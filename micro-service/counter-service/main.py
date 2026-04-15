import asyncio
import json
import os
from contextlib import asynccontextmanager

import hazelcast
from fastapi import FastAPI
from pydantic import BaseModel
from sqlalchemy import Column, Float, String, create_engine, select
from sqlalchemy.orm import Session, declarative_base

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+psycopg2://postgres:postgres@postgres:5432/appdb",
)

SERVICE_NAME = os.getenv("SERVICE_NAME", "counter-service")

HAZELCAST_MEMBERS = os.getenv(
    "HAZELCAST_MEMBERS",
    "hazelcast-1:5701,hazelcast-2:5701,hazelcast-3:5701",
).split(",")

QUEUE_NAME = os.getenv("QUEUE_NAME", "transaction-queue")

engine = create_engine(DATABASE_URL, future=True)
Base = declarative_base()

hz_client = None
hz_queue = None
consumer_task = None


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


def apply_transaction_logic(tx: TransactionIn):
    with Session(engine) as session:
        existing = session.get(AppliedTransaction, tx.transaction_id)
        if existing is not None:
            account = session.get(Account, tx.user_id)
            balance = account.balance if account else 0.0

            print(f"[{SERVICE_NAME}] Duplicate tx ignored: {tx.transaction_id}")

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

        print(
            f"[{SERVICE_NAME}] Applied tx={tx.transaction_id} "
            f"user={tx.user_id} amount={tx.amount} new_balance={account.balance}"
        )

        return {
            "status": "applied",
            "transaction_id": tx.transaction_id,
            "balance": account.balance,
        }


async def consume_queue():
    print(f"[{SERVICE_NAME}] Consumer started for queue: {QUEUE_NAME}")

    while True:
        try:
            item = await asyncio.to_thread(hz_queue.take)

            if isinstance(item, str):
                data = json.loads(item)
            else:
                data = item

            tx = TransactionIn(**data)

            print(f"[{SERVICE_NAME}] Received from queue: {data}")
            apply_transaction_logic(tx)

        except Exception as e:
            print(f"[{SERVICE_NAME}] Consumer error: {e}")
            await asyncio.sleep(1)


@asynccontextmanager
async def lifespan(app: FastAPI):
    global hz_client, hz_queue, consumer_task

    Base.metadata.create_all(bind=engine)

    hz_client = hazelcast.HazelcastClient(cluster_members=HAZELCAST_MEMBERS)
    hz_queue = hz_client.get_queue(QUEUE_NAME).blocking()

    print(f"[{SERVICE_NAME}] Connected to Hazelcast: {HAZELCAST_MEMBERS}")
    print(f"[{SERVICE_NAME}] Connected to queue: {QUEUE_NAME}")

    consumer_task = asyncio.create_task(consume_queue())

    yield

    if consumer_task:
        consumer_task.cancel()

    hz_client.shutdown()


app = FastAPI(lifespan=lifespan)


@app.get("/health")
def health():
    return {"status": "ok", "service": SERVICE_NAME}


@app.post("/apply")
def apply_transaction(tx: TransactionIn):
    return apply_transaction_logic(tx)


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