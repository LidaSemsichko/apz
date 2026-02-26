import time
import hazelcast

QUEUE_NAME = "lab-queue"

def main() -> None:
    client = hazelcast.HazelcastClient(cluster_members=["hz1:5701"])
    q = client.get_queue(QUEUE_NAME).blocking()

    for i in range(1, 101):
        q.put(i)
        print("PUT", i)
        time.sleep(0.01)

    client.shutdown()

if __name__ == "__main__":
    main()