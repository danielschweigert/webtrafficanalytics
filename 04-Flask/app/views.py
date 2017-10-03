import datetime
from flask import render_template
from flask import redirect
from flask import jsonify
from cassandra.cluster import Cluster

from app import app

CASSANDRA_RESOURCE_LOCATION = '../resources/cassandra.config'

# starting CASSANDRA session
with open(CASSANDRA_RESOURCE_LOCATION) as f:
	line1 = f.readline()
	cassandra_hosts = line1.strip().split('=')[1].split(',')
cluster = Cluster(cassandra_hosts)
session = cluster.connect('webtrafficanalytics')


@app.route('/')
@app.route('/index')
def index():
  return render_template('demo.html')


@app.route('/slides')
def slides():
    return redirect(\
    	"https://docs.google.com/presentation/d/1fibz_wxp-qUf5S8smS55-60Jl-MChfEkpLwGthqjaLg/edit#slide=id.p")


@app.route('/api/metric/<metrics>/<last_seconds>')
def get_metric(metrics, last_seconds):
	if not last_seconds.isdigit():
		return jsonify([])
	now = datetime.datetime.now()
	cutt_off_time = (now - datetime.timedelta(hours=7, seconds=int(last_seconds))).strftime('%Y-%m-%d %H:%M:%S')
	jsonresponse = []
	for metric in metrics.split(','):
		stmt = "SELECT event_time, value from metrics WHERE type = %s and event_time >= %s"
		response = session.execute(stmt, parameters=[metric, cutt_off_time])
		response_list = []
		contained_timestamps = []
		for val in response:
			response_list.append(val)
			contained_timestamps.append(val.event_time.strftime('%Y-%m-%d %H:%M:%S'))
		jsonresponse += [{"event_time": x.event_time.strftime('%Y-%m-%d %H:%M:%S'), "value": x.value, "type": metric} for x in response_list]

		# filling missing timestamps with 0. If no events were recorded, there is no entry in Cassandra
		t_i = datetime.datetime.strptime(cutt_off_time, '%Y-%m-%d %H:%M:%S')
		while t_i < (now - datetime.timedelta(hours=7, seconds=8)):
			t_s = t_i.strftime('%Y-%m-%d %H:%M:%S')
			if t_s not in contained_timestamps:
				jsonresponse.append({"event_time": t_s, "value": 0, "type": metric})
			t_i = t_i + datetime.timedelta(seconds=1)

	return jsonify(jsonresponse)

@app.route('/api/top10/<metric>')
def get_top10(metric):
	previous_minute = (datetime.datetime.now() - datetime.timedelta(hours=7, minutes=1)).strftime('%Y-%m-%d %H:%M')+':00'
	stmt = "SELECT event_time, ip, visits from visit_rank WHERE type = %s and event_time = \'" + previous_minute + "\'"
	response = session.execute(stmt, parameters=[metric])
	response_list = []
	for val in response:
		response_list.append(val)
	jsonresponse = [{"event_time": x.event_time.strftime('%Y-%m-%d %H:%M:%S'), "ip": x.ip, "value": x.visits} for x in response_list]
	return jsonify(jsonresponse)
