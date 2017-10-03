"""
Module to assist in parsing the incoming data.
"""

INT_TYPES = ['size', 'idx', 'norefer', 'noagent', 'find', 'crawler']

class DataImportFormatError(ValueError):
    """
    Custom type of ValueError used to flag consistency problems in incoming data set
    """
    pass

def parse_line_to_dict(line, header):
	"""
	Returns a parsed dictionary from a line of data.

	line: str
	header: list

	returns: dict
	"""
	fields = line.strip().split(',')
	if len(header) != len(fields):
		raise DataImportFormatError("Mismatch in header and data line in incoming data file.")
	record = {}
	for i in range(len(header)):
		value = fields[i] if header[i] not in INT_TYPES else 0 if len(fields[i]) < 1 else int(float(fields[i])) 
		record[header[i]] = value
	return record


def parse_block_to_dicts(content):
	"""
	Can be used to parse through an imported block of data (i.e. lines separated
	by '\n')
	
	Was primarily used in an earlier version.

	content: str

	returns: list of dict
	"""
	lines = content.split('\n')
	if len(lines) > 0:
		header = lines[0].strip().split(',')
	records = []
	for line in lines[1:]:
		if len(line) > 0:
			records.append(parse_line_to_dict(line, header))
	return records

