import time
import hazelcast

client = hazelcast.HazelcastClient(cluster_members=["hz1:5701"])
m = client.get_map("speed-test").blocking()

start = time.time()
for i in range(100):
    m.put(i, i)
end = time.time()

print("Time for 100 puts:", end - start)
client.shutdown()
