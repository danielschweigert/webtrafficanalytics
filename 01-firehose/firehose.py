import boto3
from boto3.session import Session
from confluent_kafka import Producer
import time


class Firehose(object):

	def __init__(self):

		# reading Kafka config file
		with open('resources/kafka.config') as f:
			line1 = f.readline()
			self.bootstrap_servers = line1.strip().split('=')[1].split(',')

			line2 = f.readline()
			self.port = line2.strip().split('=')[1]

			line3 = f.readline()
			self.topic = line3.strip().split('=')[1]

			self.producer = Producer({'bootstrap.servers': ','.join(self.bootstrap_servers), 'queue.buffering.max.messages':100000, 'queued.max.messages.kbytes':1000000000, 'acks':'all', 'enable.auto.commit':'true'})

		# report to console
		print '---'
		print 'connected to kafka bootstrap.servers:'
		for bs in self.bootstrap_servers:
			print bs
		print 'producing to topic:'
		print '-> ' + self.topic

	def start(self):
		raise NotImplementedError()


class StandardFirehose(Firehose):

	def __init__(self):
		super(StandardFirehose, self).__init__()
		
		# reading S3 config file
		with open('resources/s3.config') as f:
			line1 = f.readline()
			aws_access_key = line1.strip().split('=')[1]

			line2 = f.readline()
			aws_secret_key = line2.strip().split('=')[1]

			line3 = f.readline()
			self.bucket_name = line3.strip().split('=')[1]

		# initiating S3 session
		self.session = Session(aws_access_key_id=aws_access_key,
        	          aws_secret_access_key=aws_secret_key)
		self.s3 = self.session.resource('s3')
		self.bucket = self.s3.Bucket(self.bucket_name)

		# report to console
		print '---'
		print 'connected to S3 bucket:'
		print '-> ' + self.bucket_name

	def start(self):
		for s3_file in self.bucket.objects.all():
			if s3_file.key.lower().endswith('.csv'):

				# report to console
				print 'producing to kafka topic ' + self.topic + ' data from file: ' + s3_file.key

				obj = s3_file.Object()
				content = obj.get()['Body'].read()
				self._send_to_kafka(content)

			
	def _send_to_kafka(self, content):
		lines = content.split('\n')
		lines = lines[1:]
		i = 0
		for line in lines:
			if len(line) > 0:
				self.producer.produce(self.topic, line.strip().encode('utf-8'))
				i += 1
				if i>1000: # TODO: MAKE THIS NICER
					self.producer.flush()
					i = 0 
		
firehose = StandardFirehose()
firehose.start()