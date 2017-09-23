from boto3.session import Session
from cassandra.cluster import Cluster

S3_RESOURCE_LOCATION = '../resources/s3masterdataset.config'
CASSANDRA_RESOURCE_LOCATION = '../resources/cassandra.config'

CASSANDRA_KEYSPACE = 'webtrafficanalytics'


def get_cassandra_session():
	"""
	This provides a Cassandra session.

	returns:
	cassandra_session: 		cassandra.cluster.Session
	"""

	# setup connection to CASSANDRA
	with open(CASSANDRA_RESOURCE_LOCATION) as f:
		line1 = f.readline()
		cassandra_hosts = line1.strip().split('=')[1].split(',')

	# setup cassandra connection
	cassandra_cluster = Cluster(cassandra_hosts)
	cassandra_session = cassandra_cluster.connect(CASSANDRA_KEYSPACE)

	return cassandra_session


def get_data_connections():
	"""
	This provides the main handles necessary to send/receive data from
	S3 and Cassandra.

	returns:
	bucket:					S3.Bucket 
	cassandra_session: 		cassandra.cluster.Session
	"""

	# setup connection to master data fileset in S3
	with open(S3_RESOURCE_LOCATION) as f:
		line1 = f.readline()
		aws_access_key = line1.strip().split('=')[1]

		line2 = f.readline()
		aws_secret_key = line2.strip().split('=')[1]

		line3 = f.readline()
		bucket_name = line3.strip().split('=')[1]

	# setup connection to CASSANDRA
	with open(CASSANDRA_RESOURCE_LOCATION) as f:
		line1 = f.readline()
		cassandra_hosts = line1.strip().split('=')[1].split(',')

	# setup cassandra connection
	cassandra_cluster = Cluster(cassandra_hosts)
	cassandra_session = cassandra_cluster.connect(CASSANDRA_KEYSPACE)

	# setup connection to S3 master data set
	session = Session(aws_access_key_id=aws_access_key, aws_secret_access_key=aws_secret_key)
	s3 = session.resource('s3')
	bucket = s3.Bucket(bucket_name)

	return bucket, cassandra_session


