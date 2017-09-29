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

	# update local visits data set
	sql_statement = "select event_time, total, unique from visits_type where type = 'all' order by event_time desc limit 7200"
	rows = cassandra_session.execute(sql_statement)
	with open('04-webui/app/static/visits.csv', 'w') as f:
		f.write('time,total,unique,\n')
		for row in rows:
			f.write(row[0].strftime('%Y-%m-%d %H:%M:%S') + ', ' + str(row[1]) + ',' + str(row[2]) + ',\n')

	# update local volume data set
	sql_statement = "select event_time, crawler, human from volume_type where type = 'all' order by event_time desc limit 7200"
	rows = cassandra_session.execute(sql_statement)
	with open('04-webui/app/static/volume.csv', 'w') as f:
		f.write('time,crawler,human,\n')
		for row in rows:
			f.write(row[0].strftime('%Y-%m-%d %H:%M:%S') + ', ' + str(row[1] or '0') + ',' + str(row[2] or '0') + ',\n')

	# update top 10 visits
	previous_minute = (datetime.datetime.now() - datetime.timedelta(hours=7, minutes=1)).strftime('%Y-%m-%d %H:%M')+':00'
	sql_statement = "select event_time, ip, visits from visit_rank where type = 'clicks' and event_time = \'" + previous_minute + "\'"
	rows = cassandra_session.execute(sql_statement)
	with open('04-webui/app/static/visits_top10_clicks.csv', 'w') as f:
		f.write('time,ip,visits,\n')
		for row in rows:
			f.write(row[0].strftime('%Y-%m-%d %H:%M:%S') + ', ' + str(row[1] or '0') + ',' + str(row[2] or '0') + ',\n')

	sql_statement = "select event_time, ip, visits from visit_rank where type = 'volume' and event_time = \'" + previous_minute + "\'"
	rows = cassandra_session.execute(sql_statement)
	with open('04-webui/app/static/visits_top10_volume.csv', 'w') as f:
		f.write('time,ip,volume,\n')
		for row in rows:
			f.write(row[0].strftime('%Y-%m-%d %H:%M:%S') + ', ' + str(row[1] or '0') + ',' + str(row[2] or '0') + ',\n')

	# update distributions of http status codes
	sql_statement = "select event_time, count from code_count_3 where type = '4xx' order by event_time desc limit 7200"
	rows = cassandra_session.execute(sql_statement)
	with open('04-webui/app/static/code_count.csv', 'w') as f:
		f.write('time,count,\n')
		for row in rows:
			f.write(row[0].strftime('%Y-%m-%d %H:%M:%S') + ', ' + str(row[1] or '0') + ',\n')

	# report to console
	print 'updated at ' + str(datetime.datetime.now())
	time.sleep(1)