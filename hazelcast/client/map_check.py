import hazelcast

def main() -> None:
    client = hazelcast.HazelcastClient(cluster_members=["hz1:5701"])
    m = client.get_map("lab-map").blocking()

    size = m.size()
    print("Map size =", size)

    keys_to_check = [0, 1, 42, 500, 999, 1000]
    missing = 0

    for k in keys_to_check:
        v = m.get(k)
        print(f"{k} -> {v}")
        if v is None:
            missing += 1

    if size != 1001 or missing > 0:
        print("WARNING: potential data loss or map not filled yet.")
    else:
        print("OK: map looks consistent.")

    client.shutdown()

if __name__ == "__main__":
    main()