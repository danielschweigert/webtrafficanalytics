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


@app.route('/api/visits/<last_seconds>')
def get_visits(last_seconds):
	stmt = """SELECT event_time, total, unique from visits_type WHERE type='all' \
	order by event_time desc limit %s"""
	response = session.execute(stmt, parameters=[int(last_seconds)])
	response_list = []
	for val in response:
		response_list.append(val)
	jsonresponse = [{"event_time": x.event_time.strftime('%Y-%m-%d %H:%M:%S'), "total": x.total, "unique": x.unique} for x in response_list]
	return jsonify(jsonresponse)

@app.route('/api/volume/<last_seconds>')
def get_volume(last_seconds):
	stmt = """SELECT event_time, crawler, human from volume_type WHERE type='all' \
	order by event_time desc limit %s"""
	response = session.execute(stmt, parameters=[int(last_seconds)])
	response_list = []
	for val in response:
		response_list.append(val)
	jsonresponse = [{"event_time": x.event_time.strftime('%Y-%m-%d %H:%M:%S'), "crawler": x.crawler, "human": x.human} for x in response_list]
	return jsonify(jsonresponse)

@app.route('/api/code/<code>/<last_seconds>')
def get_code_count(code, last_seconds):
	stmt = """SELECT event_time, count from code_count_3 WHERE type=%s \
	order by event_time desc limit %s"""
	response = session.execute(stmt, parameters=[code, int(last_seconds)])
	response_list = []
	for val in response:
		response_list.append(val)
	jsonresponse = [{"event_time": x.event_time.strftime('%Y-%m-%d %H:%M:%S'), "count": x.count} for x in response_list]
	return jsonify(jsonresponse)