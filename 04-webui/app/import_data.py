from wtautils import get_cassandra_session
import datetime
import time

cassandra_session = get_cassandra_session()


while True:


	sql_statement = "select event_time, count from visits where type='total' and event_time >= '2010-01-01' and event_time < '2010-01-02'"
	rows = cassandra_session.execute(sql_statement)
	with open('static/total_visits.csv', 'w') as f:
		f.write('time,count,\n')
		for row in rows:
			f.write(row[0].strftime('%Y-%m-%d %H:%M') + ':00,' + str(row[1]) + ',' + '\n')
	print 'updated count'



	sql_statement = "select event_time, volume from volume_2 where type='total' order by event_time limit 1440";
	rows = cassandra_session.execute(sql_statement)
	with open('static/total_volume.csv', 'w') as f:
		f.write('time,volume(b),\n')
		for row in rows:
			f.write(row[0].strftime('%Y-%m-%d %H:%M') + ':00,' + str(row[1]) + ',' + '\n')
	print 'updated volume'


	time.sleep(30)