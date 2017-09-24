import boto3
from boto3.session import Session
from confluent_kafka import Producer
from kafka import KafkaProducer, KafkaClient
import time
import io
import avro.schema
from avro.io import DatumWriter
from secparse import parse_block_to_dicts


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


class StandardFirehose(Firehose):

	def __init__(self):
		super(StandardFirehose, self).__init__()

		# initiating simple producer
		self.producer = Producer({'bootstrap.servers': ','.join(self.bootstrap_servers), 'queue.buffering.max.messages':100000, 'queued.max.messages.kbytes':1000000000, 'acks':'all', 'enable.auto.commit':'true'})

		# report to console
		print '---'
		print 'connected to kafka bootstrap.servers:'
		for bs in self.bootstrap_servers:
			print bs

		print 'producing to topic:'
		print '-> ' + self.topic

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


class AvroFirehose(Firehose):

	def __init__(self):
		super(AvroFirehose, self).__init__()
		schema_path="resources/avro/sec_record.avsc"
		self.schema = avro.schema.parse(open(schema_path).read())
		self.producer = KafkaProducer(bootstrap_servers=self.bootstrap_servers[0] + ':' + self.port)
		#TODO: move into config

	def _send_to_kafka(self, content):
		records = parse_block_to_dicts(content, use_current_time_stamp=False)
		for record in records:
			writer = avro.io.DatumWriter(self.schema)
			bytes_writer = io.BytesIO()
			encoder = avro.io.BinaryEncoder(bytes_writer)
			writer.write(record, encoder)
			raw_bytes = bytes_writer.getvalue()
			self.producer.send(self.topic, raw_bytes)
			self.producer.flush()


if __name__ == "__main__":
	firehose = AvroFirehose()
	firehose.start()