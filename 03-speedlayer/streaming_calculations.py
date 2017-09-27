import os
import datetime
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement
from cassandra import ConsistencyLevel

import avro.schema
import avro.io
import io
from avro.io import BinaryDecoder

# start this job with:
# $SPARK_HOME/bin/spark-submit --master spark://ip-10-0-0-7:7077 --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.2 03-speedlayer/streaming_calculations.py 

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.2 pyspark-shell'

CASSANDRA_RESOURCE_LOCATION = 'resources/cassandra.config'
KAFKA_RESOURCE_LOCATION = 'resources/kafka.config'

CASSANDRA_KEYSPACE = 'webtrafficanalytics'
CASSANDRA_TABLE_VISITS_TYPE = 'visits_type'
CASSANDRA_TABLE_VOLUME_TYPE = 'volume_type'
CASSANDRA_TABLE_VISIT_RANK = 'visit_rank'

# obtain kafka brokers from config
with open(KAFKA_RESOURCE_LOCATION) as f:
	line1 = f.readline()
	kafka_addresses = line1.strip().split('=')[1].split(',')

	line2 = f.readline()
	kafka_port = line2.strip().split('=')[1]

	kafka_brokers = ''
	for kafka_address in kafka_addresses:
		kafka_brokers = kafka_brokers + kafka_address + ':' + kafka_port + ','
	kafka_brokers = kafka_brokers[:-1]
	
	line3 = f.readline()
	kafka_topic = line3.strip().split('=')[1]

# obtain cassandra hosts from config
with open(CASSANDRA_RESOURCE_LOCATION) as f:
	line1 = f.readline()
	cassandra_hosts = line1.strip().split('=')[1].split(',')

# load avro schema
schema_path="resources/avro/sec_record.avsc"
schema = avro.schema.parse(open(schema_path).read())

# avro decoder function
def avro_decoder(msg):
	"""
	This function is used to decode the avro messages in the 
	kafka queue when the dstream object is obtained.
	"""
	bytes_reader = io.BytesIO(msg)
	decoder = BinaryDecoder(bytes_reader)
	reader = avro.io.DatumReader(schema)
	return reader.read(decoder)

def update_sum(new_values, last_sum):
	if new_values == []:
		return None
	return sum(new_values) + (last_sum or 0)

def update_list(new_values, last_list):
	if new_values == []:
		return None
	return new_values[0] + (last_list or [])

def send_count(iter):
	cassandra_cluster = Cluster(cassandra_hosts)
	cassandra_session = cassandra_cluster.connect(CASSANDRA_KEYSPACE)
	insert_count = cassandra_session.prepare("UPDATE " + CASSANDRA_TABLE_VISITS_TYPE + " SET total = ? WHERE type = 'all' and event_time = ?")
	batch = BatchStatement(consistency_level=ConsistencyLevel.ANY)
	for record in iter:
		batch.add(insert_count, (record[1], datetime.datetime.strptime(record[0], '%Y-%m-%d %H:%M:%S')))
	cassandra_session.execute(batch)
	cassandra_cluster.shutdown()

def send_unique_count(iter):
	cassandra_cluster = Cluster(cassandra_hosts)
	cassandra_session = cassandra_cluster.connect(CASSANDRA_KEYSPACE)
	insert_count = cassandra_session.prepare("UPDATE " + CASSANDRA_TABLE_VISITS_TYPE + " SET unique = ? WHERE type = 'all' and event_time = ?")
	batch = BatchStatement(consistency_level=ConsistencyLevel.ANY)
	for record in iter:
		batch.add(insert_count, (record[1], datetime.datetime.strptime(record[0], '%Y-%m-%d %H:%M:%S')))
	cassandra_session.execute(batch)
	cassandra_cluster.shutdown()	

def send_volume(iter):
	cassandra_cluster = Cluster(cassandra_hosts)
	cassandra_session = cassandra_cluster.connect(CASSANDRA_KEYSPACE)	
	for record in iter:
		field = 'crawler' if record[0][20] == '1' else 'human'
		sql_statement = "UPDATE " + CASSANDRA_TABLE_VOLUME_TYPE + " SET " + field + " = " + str(record[1]) + " WHERE type = 'all' and event_time = \'" + record[0][0:19] + "\'"
		#print sql_statement
		cassandra_session.execute(sql_statement)
	cassandra_cluster.shutdown()

def send_click_rank(iter):
	cassandra_cluster = Cluster(cassandra_hosts)
	cassandra_session = cassandra_cluster.connect(CASSANDRA_KEYSPACE)
	insert_visit_rank = cassandra_session.prepare("INSERT INTO " + CASSANDRA_TABLE_VISIT_RANK + " (type, event_time, rank, ip, visits) VALUES ('clicks', ?, ?, ?, ?)")
	batch = BatchStatement(consistency_level=ConsistencyLevel.ANY)
	for record in iter:
		batch.add(insert_visit_rank, (datetime.datetime.strptime(record[0][0][0:16], '%Y-%m-%d %H:%M'), record[1], record[0][0].split(' ')[-1], record[0][1]))
	cassandra_session.execute(batch)
	cassandra_cluster.shutdown()

def send_volume_rank(iter):
	cassandra_cluster = Cluster(cassandra_hosts)
	cassandra_session = cassandra_cluster.connect(CASSANDRA_KEYSPACE)
	insert_visit_rank = cassandra_session.prepare("INSERT INTO " + CASSANDRA_TABLE_VISIT_RANK + " (type, event_time, rank, ip, visits) VALUES ('volume', ?, ?, ?, ?)")
	batch = BatchStatement(consistency_level=ConsistencyLevel.ANY)
	for record in iter:
		batch.add(insert_visit_rank, (datetime.datetime.strptime(record[0][0][0:16], '%Y-%m-%d %H:%M'), record[1], record[0][0].split(' ')[-1], record[0][1]))
	cassandra_session.execute(batch)
	cassandra_cluster.shutdown()

# registering the spark context
conf = SparkConf().setAppName("03-streaming_calculations")
sc = SparkContext(conf=conf)

# this is only necessary for manual run and debugging
logger = sc._jvm.org.apache.log4j
logger.LogManager.getLogger("org").setLevel( logger.Level.ERROR )
logger.LogManager.getLogger("akka").setLevel( logger.Level.ERROR )

# streaming context and checkpoint
ssc = StreamingContext(sc, 5)
ssc.checkpoint("hdfs://ec2-13-57-66-131.us-west-1.compute.amazonaws.com:9000/checkpoint/")

# initial state RDD
initialStateRDD = sc.parallelize([])

# obtaining stream from Kafka
kafkaStream = KafkaUtils.createDirectStream(ssc, [kafka_topic], {"metadata.broker.list": kafka_brokers}, valueDecoder=avro_decoder)
kafkaStream.cache()

# aggregations (by sec)
visits = kafkaStream.map(lambda x : x[1]).map(lambda x : (x['date'] + ' ' + x['time'], 1)).reduceByKey(lambda a, b : a + b).updateStateByKey(update_sum, initialRDD=initialStateRDD)
visits_ip = kafkaStream.map(lambda x : x[1]).map(lambda x : (x['date'] + ' ' + x['time'], [x['ip']])).reduceByKey(lambda a, b : a + b).updateStateByKey(update_list, initialRDD=initialStateRDD)
visits_unique = visits_ip.flatMap(lambda x : [x[0] + ' ' + xx for xx in x[1]]).map(lambda x : (x, 1)).reduceByKey(lambda a, b : None).map(lambda x : (x[0][0:19], 1)).reduceByKey(lambda a, b : a + b)
volume_crawler = kafkaStream.map(lambda x : x[1]).map(lambda x : (x['date'] + ' ' + x['time'], [(x['crawler'], x['size'])])).reduceByKey(lambda a, b : a + b).updateStateByKey(update_list, initialRDD=initialStateRDD)
volume_crawler_sum = volume_crawler.flatMap(lambda x : [x[0] + ' ' + str(xx[0]) + ' ' + str(xx[1]) for xx in x[1]]).map(lambda x : (x[0:21], int(x.split(' ')[3]))).reduceByKey(lambda a, b : a + b)

# aggregations (by min)
visits_ip_min = kafkaStream.map(lambda x : x[1]).map(lambda x : (x['date'] + ' ' + x['time'][0:5], [(x['ip'], x['size'])])).reduceByKey(lambda a, b : a + b).updateStateByKey(update_list, initialRDD=initialStateRDD)
visits_ip_min_flat = visits_ip_min.flatMap(lambda x : [x[0] + ' ' + str(xx[0]) + ' , ' + str(xx[1]) for xx in x[1]])
visits_ip_min_flat.cache()

# top 10 visitors by clicks
click_rank = visits_ip_min_flat.map(lambda x : (x.split(' , ')[0], 1)).reduceByKey(lambda a, b : a + b).transform(lambda x : x.sortBy(lambda y : (y[0][0:16], y[1]), ascending=False))
click_rank_top_10 = click_rank.transform(lambda x : x.zipWithIndex().filter(lambda x : x[1] < 10))

# top 10 visitors by volume
#visits_top_by_volume = visits_ip_min.flatMap(lambda x : [x[0] + ' ' + str(xx[0]) + ' , ' + str(xx[1]) for xx in x[1]])
volume_rank = visits_ip_min_flat.map(lambda x : (x.split(' , ')[0], int(x.split(' , ')[1]))).reduceByKey(lambda a, b : a + b).transform(lambda x : x.sortBy(lambda y : (y[0][0:16], y[1]), ascending=False))
volume_rank_top_10 = volume_rank.transform(lambda x : x.zipWithIndex().filter(lambda x : x[1] < 10))
volume_rank_top_10.pprint()

# insert to Cassandra database
visits.foreachRDD(lambda rdd: rdd.foreachPartition(send_count))
visits_unique.foreachRDD(lambda rdd: rdd.foreachPartition(send_unique_count))
volume_crawler_sum.foreachRDD(lambda rdd: rdd.foreachPartition(send_volume))
click_rank_top_10.foreachRDD(lambda rdd: rdd.foreachPartition(send_click_rank))
volume_rank_top_10.foreachRDD(lambda rdd: rdd.foreachPartition(send_volume_rank))

# start
ssc.start()
ssc.awaitTermination()