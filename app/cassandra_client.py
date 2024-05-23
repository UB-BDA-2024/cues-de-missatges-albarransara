from cassandra.cluster import Cluster

class CassandraClient:
    def __init__(self, hosts):
        self.cluster = Cluster(hosts,protocol_version=4)
        self.session = self.cluster.connect()
        # First we will create a Cassandra keyspae for our tables
        self.session.execute("CREATE KEYSPACE IF NOT EXISTS sensor WITH REPLICATION = { 'class': 'SimpleStrategy', 'replication_factor': 1};")
        # Then we will create our tables, the first one will be the temperatures one
        self.session.execute("CREATE TABLE IF NOT EXISTS sensor.temperature(id INT, last_seen TIMESTAMP, temperature FLOAT, PRIMARY KEY(id,last_seen));")
        # Then we will create the type of sensor quantity table
        self.session.execute("CREATE TABLE IF NOT EXISTS sensor.quantity(type_sensor text PRIMARY KEY, quantity counter);")
        # Finally, we will create the low_battery table
        self.session.execute("CREATE TABLE IF NOT EXISTS sensor.battery(id INT PRIMARY KEY, battery_level FLOAT);")

    def get_session(self):
        return self.session

    def close(self):
        self.cluster.shutdown()

    def execute(self, query):
        return self.get_session().execute(query)

