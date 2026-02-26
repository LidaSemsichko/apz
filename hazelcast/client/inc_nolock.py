import time
import hazelcast

KEY = "key"
ITERS = 10_000
MAP_NAME = "counter-map"

def main() -> None:
    client = hazelcast.HazelcastClient(cluster_members=["hz1:5701"])
    m = client.get_map(MAP_NAME).blocking()

    m.put_if_absent(KEY, 0)

    t0 = time.time()
    for _ in range(ITERS):
        v = m.get(KEY)
        v = int(v) + 1
        m.put(KEY, v)
    dt = time.time() - t0

    print(f"[NOLOCK] done {ITERS} iters, time={dt:.3f}s, current={m.get(KEY)}")

    client.shutdown()

if __name__ == "__main__":
    main()