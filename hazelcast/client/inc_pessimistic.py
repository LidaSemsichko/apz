import time
import hazelcast

KEY = "key"
ITERS = 10_000
MAP_NAME = "counter-map-lock"

def main() -> None:
    client = hazelcast.HazelcastClient(cluster_members=["hz1:5701"])
    m = client.get_map(MAP_NAME).blocking()

    m.put_if_absent(KEY, 0)

    t0 = time.time()
    for _ in range(ITERS):
        m.lock(KEY)
        try:
            v = m.get(KEY)
            v = int(v) + 1
            m.put(KEY, v)
        finally:
            m.unlock(KEY)

    dt = time.time() - t0
    print(f"[PESSIMISTIC] done {ITERS} iters, time={dt:.3f}s, current={m.get(KEY)}")

    client.shutdown()

if __name__ == "__main__":
    main()