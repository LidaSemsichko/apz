import hazelcast

QUEUE_NAME = "lab-queue"

def main() -> None:
    client = hazelcast.HazelcastClient(cluster_members=["hz1:5701"])
    q = client.get_queue(QUEUE_NAME).blocking()

    while True:
        item = q.take()
        print("GOT", item)

if __name__ == "__main__":
    main()