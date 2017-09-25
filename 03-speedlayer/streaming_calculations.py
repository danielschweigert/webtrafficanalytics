import os
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from cassandra.cluster import Cluster

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
CASSANDRA_TABLE_VISITS = 'visits'
CASSANDRA_TABLE_VOLUME = 'volume'
CASSANDRA_TABLE_VISITS_TYPE = 'visits_type'
CASSANDRA_TABLE_VOLUME_TYPE = 'volume_type'


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
	for record in iter:
		sql_statement = "UPDATE " + CASSANDRA_TABLE_VISITS_TYPE + " SET total = " + str(record[1]) + " WHERE type = 'all' and event_time = \'" + record[0] + "\'" + ";"
		cassandra_session.execute(sql_statement)	
	cassandra_cluster.shutdown()

def send_volume(iter):
	cassandra_cluster = Cluster(cassandra_hosts)
	cassandra_session = cassandra_cluster.connect(CASSANDRA_KEYSPACE)
	for record in iter:
		sql_statement = "INSERT INTO " + CASSANDRA_TABLE_VOLUME + " (type, event_time, volume) VALUES ('total', \'" + str(record[0]) + "\', " + str(record[1]) + ")"
		cassandra_session.execute(sql_statement)
	cassandra_cluster.shutdown()

def send_unique_count(iter):
	cassandra_cluster = Cluster(cassandra_hosts)
	cassandra_session = cassandra_cluster.connect(CASSANDRA_KEYSPACE)
	for record in iter:
		sql_statement = "UPDATE " + CASSANDRA_TABLE_VISITS_TYPE + " SET unique = " + str(record[1]) + " WHERE type = 'all' and event_time = \'" + record[0] + "\'" + ";"
		cassandra_session.execute(sql_statement)
	cassandra_cluster.shutdown()	

def send_volume_crawler(iter):
	cassandra_cluster = Cluster(cassandra_hosts)
	cassandra_session = cassandra_cluster.connect(CASSANDRA_KEYSPACE)
	for record in iter:
		field = 'crawler' if record[0][20] == '1' else 'human'
		sql_statement = "UPDATE " + CASSANDRA_TABLE_VOLUME_TYPE + " SET " + field + " = " + str(record[1]) + " WHERE type = 'all' and event_time = \'" + record[0][0:19] + "\'" + ";"
		cassandra_session.execute(sql_statement)	
	cassandra_cluster.shutdown()	


# registering the spark context
conf = SparkConf().setAppName("03-streaming_calculations")
sc = SparkContext(conf=conf)

# this is only necessary for manual run and debugging
logger = sc._jvm.org.apache.log4j
logger.LogManager.getLogger("org").setLevel( logger.Level.ERROR )
logger.LogManager.getLogger("akka").setLevel( logger.Level.ERROR )

# streaming context and checkpoint
ssc = StreamingContext(sc, 1)
ssc.checkpoint("hdfs://ec2-13-57-66-131.us-west-1.compute.amazonaws.com:9000/checkpoint/")

# initial state RDD
initialStateRDD = sc.parallelize([])

# obtaining stream from Kafka
kafkaStream = KafkaUtils.createDirectStream(ssc, [kafka_topic], {"metadata.broker.list": kafka_brokers}, valueDecoder=avro_decoder)

# aggregations
visits = kafkaStream.map(lambda x : x[1]).map(lambda x : (x['date'] + ' ' + x['time'], 1)).reduceByKey(lambda a, b : a + b).updateStateByKey(update_sum, initialRDD=initialStateRDD)
volume = kafkaStream.map(lambda x : x[1]).map(lambda x : (x['date'] + ' ' + x['time'], x['size'])).reduceByKey(lambda a, b : a + b).updateStateByKey(update_sum, initialRDD=initialStateRDD)
volume_crawler = kafkaStream.map(lambda x : x[1]).map(lambda x : (x['date'] + ' ' + x['time'], [(x['crawler'], x['size'])])).reduceByKey(lambda a, b : a + b).updateStateByKey(update_list, initialRDD=initialStateRDD)
volume_crawler_sum = volume_crawler.flatMap(lambda x : [x[0] + ' ' + str(xx[0]) + ' ' + str(xx[1]) for xx in x[1]]).map(lambda x : (x[0:21], int(x.split(' ')[3]))).reduceByKey(lambda a, b : a + b)
visits_ip = kafkaStream.map(lambda x : x[1]).map(lambda x : (x['date'] + ' ' + x['time'], [x['ip']])).reduceByKey(lambda a, b : a + b).updateStateByKey(update_list, initialRDD=initialStateRDD)
visits_ip_count = visits_ip.flatMap(lambda x : [x[0] + ' ' + xx for xx in x[1]]).map(lambda x : (x, 1)).reduceByKey(lambda a, b : a + b)
visits_unique = visits_ip_count.map(lambda x : (x[0][0:19], 1)).reduceByKey(lambda a, b : a + b)

# insert to Cassandra database
visits.foreachRDD(lambda rdd: rdd.foreachPartition(send_count))
volume.foreachRDD(lambda rdd: rdd.foreachPartition(send_volume))
visits_unique.foreachRDD(lambda rdd: rdd.foreachPartition(send_unique_count))
volume_crawler_sum.foreachRDD(lambda rdd: rdd.foreachPartition(send_volume_crawler))

# start
ssc.start()
ssc.awaitTermination()