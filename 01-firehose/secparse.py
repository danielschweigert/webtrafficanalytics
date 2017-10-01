import time
import datetime
from datetime import timedelta

INT_TYPES = ['size', 'idx', 'norefer', 'noagent', 'find', 'crawler']


class DataImportFormatError(ValueError):
    """
    Custom type of ValueError used to flag consistency problems in incoming data set
    """
    pass


def parse_line_to_dict(line, header, use_current_time_stamp=False):
	fields = line.strip().split(',')
	if len(header) != len(fields):
		raise DataImportFormatError("Mismatch in header and data line in incoming data file.")
	record = {}
	for i in range(len(header)):
		value = fields[i] if header[i] not in INT_TYPES else 0 if len(fields[i]) < 1 else int(float(fields[i])) 
		record[header[i]] = value
	if use_current_time_stamp:
		now = datetime.datetime.now()
		now = now + timedelta(hours=-7)
		record['date'] = now.strftime('%Y-%m-%d')
		record['time'] = now.strftime('%H:%M:%S')
	return record


def parse_block_to_dicts(content, use_current_time_stamp=False):
	lines = content.split('\n')
	if len(lines) > 0:
		header = lines[0].strip().split(',')
	records = []
	for line in lines[1:]:
		if len(line) > 0:
			records.append(parse_line_to_dict(line, header, use_current_time_stamp))
	return records

