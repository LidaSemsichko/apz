import hazelcast

client = hazelcast.HazelcastClient(cluster_members=["hz1:5701"])
m = client.get_map("counter-map-opt").blocking()
print("FINAL =", m.get("key"))
client.shutdown()
