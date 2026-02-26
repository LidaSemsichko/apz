import hazelcast

client = hazelcast.HazelcastClient(cluster_members=["hz1:5701"])
m = client.get_map("counter-map-opt").blocking()
m.put("key", 0)
print("reset OK (counter-map-opt)")
client.shutdown()
