import os
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from cassandra.cluster import Cluster

# start this job with:
# $SPARK_HOME/bin/spark-submit --master spark://ip-10-0-0-7:7077 --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.2 03-speedlayer/stream_total_visits.py 

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.2 pyspark-shell'

CASSANDRA_RESOURCE_LOCATION = 'resources/cassandra.config'
KAFKA_RESOURCE_LOCATION = 'resources/kafka.config'

CASSANDRA_KEYSPACE = 'webtrafficanalytics'
CASSANDRA_TABLE = 'volume'

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



def updateFunc(new_values, last_sum):
	if new_values == []:
		return None
	return sum(new_values) + (last_sum or 0)

def sendPartition(iter):
	cassandra_cluster = Cluster(cassandra_hosts)
	cassandra_session = cassandra_cluster.connect(CASSANDRA_KEYSPACE)
	for record in iter:
		sql_statement = "INSERT INTO " + CASSANDRA_TABLE + " (type, event_time, volume) VALUES ('total', \'" + str(record[0]) + "\', " +str(record[1])+ ")"
		cassandra_session.execute(sql_statement)
	cassandra_cluster.shutdown()


# registering the spark context
conf = SparkConf().setAppName("03-stream_total_volume")
sc = SparkContext(conf=conf)

# this is only necessary for manual run and debugging
logger = sc._jvm.org.apache.log4j
logger.LogManager.getLogger("org").setLevel( logger.Level.ERROR )
logger.LogManager.getLogger("akka").setLevel( logger.Level.ERROR )

# registering the streaming context
ssc = StreamingContext(sc, 1)
ssc.checkpoint("hdfs://ec2-13-57-66-131.us-west-1.compute.amazonaws.com:9000/checkpoint/")

# for stateful streaming an initial RDD is required
initialStateRDD = sc.parallelize([])

# dstream from the kafka topic
kafkaStream = KafkaUtils.createDirectStream(ssc, [kafka_topic], {"metadata.broker.list": kafka_brokers})

# aggregation on the dstream --> updating the state
lines = kafkaStream.map(lambda x: x[1]).map(lambda x : (x.split(',')[1] + ' ' + str(x.split(',')[2][:5]) + '-0800', int(float(x.split(',')[8])) if x.split(',')[8].replace('.','',1).isdigit() else 0)).reduceByKey(lambda a, b : a + b).updateStateByKey(updateFunc, initialRDD=initialStateRDD)

# sending the results to Cassandra (by partion)
lines.foreachRDD(lambda rdd: rdd.foreachPartition(sendPartition))


ssc.start()
ssc.awaitTermination()