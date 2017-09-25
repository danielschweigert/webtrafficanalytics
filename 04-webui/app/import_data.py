import datetime
import time
from cassandra.cluster import Cluster


# open Cassandra connection
CASSANDRA_RESOURCE_LOCATION = 'resources/cassandra.config'
CASSANDRA_KEYSPACE = 'webtrafficanalytics'

with open(CASSANDRA_RESOURCE_LOCATION) as f:
	line1 = f.readline()
	cassandra_hosts = line1.strip().split('=')[1].split(',')

cassandra_cluster = Cluster(cassandra_hosts)
cassandra_session = cassandra_cluster.connect(CASSANDRA_KEYSPACE)

# continuous querrying of updates
while True:

	sql_statement = "select event_time, total, unique from visits_type where type = 'all' order by event_time desc limit 7200"
	rows = cassandra_session.execute(sql_statement)
	with open('04-webui/app/static/visits.csv', 'w') as f:
		f.write('time,total,unique,\n')
		for row in rows:
			f.write(row[0].strftime('%Y-%m-%d %H:%M:%S') + ', ' + str(row[1]) + ',' + str(row[2]) + ',\n')

	time.sleep(1)