import asyncio
import time
import uuid
from typing import Dict, Any

import httpx

FACADE = "http://localhost:8000"

CLIENTS = 10
REQS_PER_CLIENT = 10_000
TIMEOUT = httpx.Timeout(connect=10.0, read=120.0, write=10.0, pool=10.0)


async def post_one_with_retry(
    client: httpx.AsyncClient,
    user_id: str,
    amount: int,
    txid: str,
) -> None:
    while True:
        try:
            r = await client.post(
                f"{FACADE}/transaction",
                json={
                    "user_Id": user_id,
                    "amount": amount,
                    "transaction_id": txid,
                },
            )

            if r.status_code >= 500:
                await asyncio.sleep(0.01)
                continue

            r.raise_for_status()
            return

        except (
            httpx.ReadTimeout,
            httpx.ConnectTimeout,
            httpx.PoolTimeout,
            httpx.ConnectError,
        ):
            await asyncio.sleep(0.01)


async def worker(client_id: int, user_id: str, n: int, amount: int) -> None:
    async with httpx.AsyncClient(timeout=TIMEOUT) as client:
        for i in range(n):
            txid = f"{user_id}-{client_id}-{i}-{uuid.uuid4()}"
            await post_one_with_retry(client, user_id, amount, txid)


async def get_accounts() -> Dict[str, float]:
    async with httpx.AsyncClient(timeout=TIMEOUT) as client:
        r = await client.get(f"{FACADE}/accounts")
        r.raise_for_status()
        data = r.json()
        balances = data.get("balances", {})
        return {str(k): float(v) for k, v in balances.items()}


async def reset_metrics() -> None:
    async with httpx.AsyncClient(timeout=TIMEOUT) as client:
        r = await client.post(f"{FACADE}/metrics/reset")
        r.raise_for_status()


async def get_metrics() -> Dict[str, Any]:
    async with httpx.AsyncClient(timeout=TIMEOUT) as client:
        r = await client.get(f"{FACADE}/metrics")
        r.raise_for_status()
        return r.json()


async def reset_system() -> None:
    async with httpx.AsyncClient(timeout=TIMEOUT) as client:
        r = await client.post(f"{FACADE}/system/reset")
        r.raise_for_status()


def rps(total_requests: int, total_s: float) -> float:
    return total_requests / total_s if total_s > 0 else 0.0


def assert_exact(value: float, expected: float, label: str) -> None:
    eps = 1e-9
    if abs(value - expected) > eps:
        raise AssertionError(f"{label}: expected EXACT {expected}, got {value}")


async def scenario_1_distinct_users() -> None:
    """
    10 clients concurrently, each sends 10k tx +1 to their OWN account.
    Expect each user_i balance = 10k exactly.
    """
    print("\n=== Scenario 1: 10 users, each ends with 10k ===")
    await reset_system()
    await reset_metrics()

    users = [f"user{i+1}" for i in range(CLIENTS)]
    tasks = [
        asyncio.create_task(worker(i, users[i], REQS_PER_CLIENT, 1))
        for i in range(CLIENTS)
    ]

    total_requests = CLIENTS * REQS_PER_CLIENT
    start = time.perf_counter()
    await asyncio.gather(*tasks)
    total_s = time.perf_counter() - start

    balances = await get_accounts()

    for u in users:
        got = balances.get(u, 0.0)
        assert_exact(got, float(REQS_PER_CLIENT), f"Balance for {u}")

    m = await get_metrics()
    print(f"Total requests: {total_requests}")
    print(f"Total time (s): {total_s:.3f}")
    print(f"RPS: {rps(total_requests, total_s):.2f}")
    print("Facade remote call timing (accumulated):", m)


async def scenario_2_same_user() -> None:
    """
    10 clients concurrently, each sends 10k tx +1 to SAME account.
    Expect user1 balance = 100k exactly.
    """
    print("\n=== Scenario 2: 1 user, ends with 100k ===")
    await reset_system()
    await reset_metrics()

    target_user = "scenario2_user"
    tasks = [
        asyncio.create_task(worker(i, target_user, REQS_PER_CLIENT, 1))
        for i in range(CLIENTS)
    ]

    total_requests = CLIENTS * REQS_PER_CLIENT
    start = time.perf_counter()
    await asyncio.gather(*tasks)
    total_s = time.perf_counter() - start

    balances = await get_accounts()
    expected = float(CLIENTS * REQS_PER_CLIENT)
    got = balances.get(target_user, 0.0)
    assert_exact(got, expected, f"Balance for {target_user}")

    m = await get_metrics()
    print(f"Total requests: {total_requests}")
    print(f"Total time (s): {total_s:.3f}")
    print(f"RPS: {rps(total_requests, total_s):.2f}")
    print("Facade remote call timing (accumulated):", m)


async def main() -> None:
    async with httpx.AsyncClient(timeout=10.0) as client:
        r = await client.get(f"{FACADE}/health")
        r.raise_for_status()

    # await scenario_1_distinct_users()
    await scenario_2_same_user()

    print("\nALL LOAD TESTS PASSED. Balances are EXACT.")


if __name__ == "__main__":
    asyncio.run(main())