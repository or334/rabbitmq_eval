#!/usr/bin/env python
import pika, sys, os
import dbHandler
import json, re, time

def main():
	connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
	channel = connection.channel()
	channel.queue_declare(queue='producer')
	channel.queue_declare(queue='grapher')
	SQLDB = dbHandler.dbHandler('test.db')
	
	def read_csv_file(location):
		ALL_KEY_NAMES=[]
		ALL_KEY_TYPES=[]
		ALL_VALUES=[]
		file_data = ALL_KEY_NAMES,ALL_KEY_TYPES,ALL_VALUES
		try:
			file_data = open(location).read()
		except Exception as e:
			print(f'[ERROR] Unable to read CSV file {location}')
			print(str(e))
			return ALL_KEY_NAMES,ALL_KEY_TYPES,ALL_VALUES
		result = file_data.split('\n')
		if len(result) == 0: return ALL_KEY_NAMES,ALL_KEY_TYPES,ALL_VALUES
		
		#Find all possible Keys
		header_row = result[0].split(',')
		if len(header_row) == 0: return ALL_KEY_NAMES,ALL_KEY_TYPES,ALL_VALUES
		for key in header_row:
			tmp_key = key.lower()
			if tmp_key not in ALL_KEY_NAMES:
				ALL_KEY_NAMES.append(tmp_key)
				ALL_KEY_TYPES.append("UNKNOWN")
		value_rows = result[1:]
		for elem in value_rows:
			ONE_ROW = []
			if '"' in elem:
				#Parse the string that may or may not contain a comma
				index = 0
				ONE_ROW.append('')
				mid_quote = False
				for character in elem:
					if character == ',' and not mid_quote:
						index+=1
						ONE_ROW.append('')
						continue
					if character == '"':
						mid_quote = not mid_quote
						continue
					ONE_ROW[index]+=character
				ALL_VALUES.append(ONE_ROW)
				continue
			value_data = elem.split(',')
			if len(value_data) != len(header_row): continue
			for value in value_data:
				ONE_ROW.append(value)
			ALL_VALUES.append(ONE_ROW)
		if len(ALL_VALUES) > 0:
			for one_row in ALL_VALUES:
				index = 0
				for value in one_row:
					if ALL_KEY_TYPES[index] == 'TEXT':
						index+=1
						continue
					match = re.match(r'^[0-9]+\.[0-9]+$', value, re.I)
					if match != None:
						ALL_KEY_TYPES[index]='REAL'
						#print(f"{ALL_KEY_NAMES[index]} 'REAL'")
						index+=1
						continue
					match = re.match('^[0-9]+$', value, re.I)
					if match != None:
						ALL_KEY_TYPES[index]='INT'
						#print(f"{ALL_KEY_NAMES[index]} 'INT'")
						index+=1
						continue
					ALL_KEY_TYPES[index]='TEXT'
					#print(f"{ALL_KEY_NAMES[index]} 'TEXT'")
					index+=1
		#print(ALL_KEY_NAMES)
		#print(ALL_KEY_TYPES)
		return ALL_KEY_NAMES,ALL_KEY_TYPES,ALL_VALUES
	
	def read_json_file(location):
		ALL_KEY_NAMES=[]
		ALL_KEY_TYPES=[]
		ALL_VALUES=[]
		json_string = None
		try:
			json_string = open(location).read()
		except Exception as e:
			print(f'[ERROR] Unable to read JSON file {location}')
			print(str(e))
			return ALL_KEY_NAMES,ALL_KEY_TYPES,ALL_VALUES
		
		result = None
		try:
			result = json.loads(json_string)
		except Exception as e:
			print(f'[ERROR] Unable to parse JSON file {location}')
			print(str(e))
			return ALL_KEY_NAMES,ALL_KEY_TYPES,ALL_VALUES
		if type(result) != list: return ALL_KEY_NAMES,ALL_KEY_TYPES,ALL_VALUES
		if len(result) == 0: return ALL_KEY_NAMES,ALL_KEY_TYPES,ALL_VALUES
		
		#Find all possible Keys
		for elem in result:
			if type(elem) != dict: continue
			for key in elem.keys():
				tmp_key = key.lower()
				if tmp_key not in ALL_KEY_NAMES:
					ALL_KEY_NAMES.append(tmp_key)
					ALL_KEY_TYPES.append("UNKNOWN")
		for elem in result:
			if type(elem) != dict: continue
			ONE_ROW = []
			for key in ALL_KEY_NAMES:
				ONE_ROW.append('NULL')
			for key in elem.keys():
				tmp_key = key.lower()
				index = ALL_KEY_NAMES.index(tmp_key)
				ONE_ROW[index]=elem[key]
			ALL_VALUES.append(ONE_ROW)
		if len(ALL_VALUES) > 0:
			for one_row in ALL_VALUES:
				index = 0
				for value in one_row:
					if ALL_KEY_TYPES[index] == 'TEXT':
						index+=1
						continue
					match = re.match(r'^[0-9]+\.[0-9]+$', value, re.I)
					if match != None:
						ALL_KEY_TYPES[index]='REAL'
						#print(f"{ALL_KEY_NAMES[index]} 'REAL'")
						index+=1
						continue
					match = re.match('^[0-9]+$', value, re.I)
					if match != None:
						ALL_KEY_TYPES[index]='INT'
						#print(f"{ALL_KEY_NAMES[index]} 'INT'")
						index+=1
						continue
					ALL_KEY_TYPES[index]='TEXT'
					#print(f"{ALL_KEY_NAMES[index]} 'TEXT'")
					index+=1
		#print(ALL_KEY_NAMES)
		#print(ALL_KEY_TYPES)
		return ALL_KEY_NAMES,ALL_KEY_TYPES,ALL_VALUES

	def consumer_cb(ch, meth, prop, bod):
		bod = bod.decode("utf-8")
		#print(f'\tconsumer_cb received {bod}')
		location, file_type, table_name = None,None,None
		try:
			location, file_type, table_name = bod.split(',')
		except:
			print(f'[ERROR] Unable to parse {bod}')
			return
		#Read the file and parse the data for CSV or JSON
		ALL_KEY_NAMES,ALL_KEY_TYPES,ALL_VALUES = None,None,None
		if file_type == 'JSON':
			ALL_KEY_NAMES,ALL_KEY_TYPES,ALL_VALUES = read_json_file(location)
		elif file_type == 'CSV':
			ALL_KEY_NAMES,ALL_KEY_TYPES,ALL_VALUES = read_csv_file(location)
		else:
			print(f'[ERROR] Unknown File Type {file_type} for {location}')
			return
		if ALL_KEY_TYPES == None or "UNKNOWN" in ALL_KEY_TYPES:
			print(f'[ERROR] UNKNOWN KEY TYPE')
			return
		#Insert the data into the SQL Lite Database
		if len(ALL_KEY_NAMES) == 0 or len(ALL_VALUES) == 0:
			print(f'[ERROR] Empty Keys or Values')
			return
		
		
		SQLDB.create_table(table_name,ALL_KEY_NAMES,ALL_KEY_TYPES)
		SQLDB.insert_into_table(table_name,ALL_KEY_NAMES,ALL_KEY_TYPES,ALL_VALUES)
		
		#Send Update Complete flag to grapher
		channel.basic_publish(exchange='', routing_key='grapher', body='complete')
		print(f'[CONSUMER] Completed {location}')
		#time.sleep(2)

	channel.basic_consume(queue='producer', on_message_callback=consumer_cb, auto_ack=True)

	print('Waiting for producer...')
	channel.start_consuming()

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)