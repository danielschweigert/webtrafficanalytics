"""
Produces a flow of web log raw data to a Kafka topic. The original data set has
historic timestamps, which are being replaced with current timestamps to simulate
a real-time process.

Prior to ingestion into Kafka, the data is serialized using the Avro framework.
"""

import os
import io
import datetime
from datetime import timedelta
from boto3.session import Session
import avro.schema
from avro.io import DatumWriter
from secparse import parse_block_to_dicts
from kafka import KafkaProducer, KafkaClient
from secparse import parse_line_to_dict
from secparse import DataImportFormatError
import random
import time
import math

RAW_FILE_PATH = 'data'
SCHEMA_PATH= 'resources/avro/sec_record.avsc'
KAFKA_CONFIG_PATH = 'resources/kafka.config'

with open(KAFKA_CONFIG_PATH) as f:
	line1 = f.readline()
	bootstrap_servers = line1.strip().split('=')[1].split(',')

	line2 = f.readline()
	port = line2.strip().split('=')[1]

	line3 = f.readline()
	topic = line3.strip().split('=')[1]

# Kafka producer
producer = KafkaProducer(bootstrap_servers=bootstrap_servers[0] + ':' + port, acks=0)

# Avro schema
schema = avro.schema.parse(open(SCHEMA_PATH).read())

# starttime for periodicity
start = datetime.datetime.now() + timedelta(hours=-7)

# report to console
print 'data flow started: ' + str(datetime.datetime.now())
print 'bootstrap_servers: ' + ','.join(bootstrap_servers)
print 'port: ' + port
print 'Kafka topic: ' + topic
i = 0
while True:
	print 'cycle starts ' + str(datetime.datetime.now())
	for file_path in os.listdir(RAW_FILE_PATH):
		with open(os.path.join(RAW_FILE_PATH, file_path), 'r') as f:
			header = f.readline().strip().split(',')
			for line in f:
				try:
					record = parse_line_to_dict(line, header)
				except DataImportFormatError as dife:
					print 'error in file : ' + file_path
				now = datetime.datetime.now()
				now = now + timedelta(hours=-7)
				time.sleep(abs(math.sin(math.pi*(now - start).total_seconds()/700.0))*abs(random.gauss(0.001, 0.001)))
				record['date'] = now.strftime('%Y-%m-%d')
				record['time'] = now.strftime('%H:%M:%S')
				writer = avro.io.DatumWriter(schema)
				bytes_writer = io.BytesIO()
				encoder = avro.io.BinaryEncoder(bytes_writer)
				writer.write(record, encoder)
				raw_bytes = bytes_writer.getvalue()
				producer.send(topic, raw_bytes)
				i += 1
				if i >= 5000:
					producer.flush()
					i = 0





