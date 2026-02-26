import time
import hazelcast

KEY = "key"
ITERS = 10_000
MAP_NAME = "counter-map-opt"

def main() -> None:
    client = hazelcast.HazelcastClient(cluster_members=["hz1:5701"])
    m = client.get_map(MAP_NAME).blocking()

    m.put_if_absent(KEY, 0)

    t0 = time.time()
    for _ in range(ITERS):
        while True:
            old = int(m.get(KEY))
            new = old + 1
            # True, якщо значення було рівно old і ми його замінили на new
            if m.replace_if_same(KEY, old, new):
                break

    dt = time.time() - t0
    print(f"[OPTIMISTIC/CAS] done {ITERS} iters, time={dt:.3f}s, current={m.get(KEY)}")

    client.shutdown()

if __name__ == "__main__":
    main()