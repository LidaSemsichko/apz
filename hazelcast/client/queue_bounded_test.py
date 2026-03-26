import hazelcast

QUEUE_NAME = "lab-queue"

def main():
    client = hazelcast.HazelcastClient(cluster_members=["hz1:5701"])
    q = client.get_queue(QUEUE_NAME).blocking()

    cleared = 0
    while True:
        item = q.poll()
        if item is None:
            break
        cleared += 1

    print("cleared:", cleared)

    inserted = 0
    failed = 0

    for i in range(1, 101):
        ok = q.offer(i, 0)
        if ok:
            inserted += 1
        else:
            failed += 1

    print("inserted:", inserted)
    print("failed:", failed)
    print("queue size:", q.size())

    client.shutdown()

if __name__ == "__main__":
    main()
