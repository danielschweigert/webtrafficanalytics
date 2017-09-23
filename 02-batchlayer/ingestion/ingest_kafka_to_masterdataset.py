import boto3
from boto3.session import Session
from confluent_kafka import Consumer, KafkaError


KAFKA_RESOURCE_LOCATION = '../../resources/kafka.config'
S3MASTER_RESOURCE_LOCATION = '../../resources/s3masterdataset.config'

class Ingestion(object):

	def __init__(self):

		# reading Kafka config file
		with open(KAFKA_RESOURCE_LOCATION) as f:
			line1 = f.readline()
			self.bootstrap_servers = line1.strip().split('=')[1].split(',')

			line2 = f.readline()
			self.topic = line2.strip().split('=')[1]

			config = {}
			config['bootstrap.servers'] = ','.join(self.bootstrap_servers)
			config['group.id'] = 'group1'
			config['default.topic.config'] = {'auto.offset.reset': 'smallest'}
			self.c = Consumer(config)
			self.c.subscribe([self.topic])

		# report to console
		print '---'
		print 'connected to kafka bootstrap.servers:'
		for bs in self.bootstrap_servers:
			print bs
		print 'subscribed to topic:'
		print self.topic

		# reading S3 config file
		with open(S3MASTER_RESOURCE_LOCATION) as f:
			line1 = f.readline()
			aws_access_key = line1.strip().split('=')[1]

			line2 = f.readline()
			aws_secret_key = line2.strip().split('=')[1]

			line3 = f.readline()
			self.bucket_name = line3.strip().split('=')[1]

		# initiating S3 session
		self.session = Session(aws_access_key_id=aws_access_key,
        	          aws_secret_access_key=aws_secret_key)
		self.s3 = self.session.client('s3')
		
		# report to console
		print '---'
		print 'connected to S3 bucket:'
		print self.bucket_name

	def start(self):
		msg = self.c.poll()
		if not msg.error():
			line = msg.value().decode('utf-8')
			date = line.split(',')[1]
		elif msg.error().code() != KafkaError._PARTITION_EOF:
			print(msg.error())			
		tmp_file_path = '/tmp/consumer.csv'
		ftemp = open(tmp_file_path, 'w')
		ftemp.write(line)
		ftemp.write('\n')
		running = True
		while running:
			msg = self.c.poll()
			if not msg.error():
				line = msg.value().decode('utf-8')
				new_date = line.split(',')[1]
				if new_date != date:
					ftemp.close()
					target = date + '.csv'
					print target
					self.s3.upload_file(tmp_file_path, self.bucket_name, target)
					ftemp = open(tmp_file_path, 'w')
					date = new_date
				ftemp.write(line)
				ftemp.write('\n')

			elif msg.error().code() != KafkaError._PARTITION_EOF:
				print(msg.error())
				running = False

		ftemp.close()


ingestion = Ingestion()
ingestion.start()