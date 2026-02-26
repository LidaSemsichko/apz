import hazelcast

def main() -> None:
    client = hazelcast.HazelcastClient(cluster_members=["hz1:5701"])

    m = client.get_map("lab-map").blocking()

    for i in range(0, 1001):
        m.put(i, f"value-{i}")

    print("Inserted keys 0..1000 into 'lab-map'")
    print("Map size =", m.size())

    client.shutdown()

if __name__ == "__main__":
    main()