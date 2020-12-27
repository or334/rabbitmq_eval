#!/usr/bin/env python
import pika, time
import json, os, random
RUN_TESTS=False
'''
TEST 5: INVALID FILE TYPE
TEST 4: INVALID TABLE NAME
TEST 3: MISSING FILES
TEST 2: MISSING DATA
TEST 1: STRESS TEST
'''

def main():
	connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
	channel = connection.channel()
	channel.queue_declare(queue='producer')

	#the location of a file,
	#the file type (CSV or JSON),
	#and a table name to which the files must be loaded
	data_list = [
		['invoices_2009.json','JSON','invoices_2009'],
		['invoices_2010.json','JSON','invoices_2010'],
		['invoices_2011.json','JSON','invoices_2011'],
		['invoices_2012.csv','CSV','invoices_2012'],
		['invoices_2013.csv','CSV','invoices_2013']
	]
	for location, file_type, table_name in data_list:
		channel.basic_publish(exchange='', routing_key='producer', body=f'{location},{file_type},{table_name}')
		print(f'sent {location}')
		#time.sleep(2)
	connection.close()
	
def create_csv_file(year,num_lines,chaos=False):
	write_to_file='InvoiceId,CustomerId,InvoiceDate,BillingAddress,BillingCity,BillingState,BillingCountry,BillingPostalCode,Total\n'
	InvoiceId=1
	for i in range(num_lines):
		#250,55,2012-01-01 00:00:00,421 Bourke Street,Sidney,NSW,Australia,2010,2703.14
		CustomerId=random.randint(1, num_lines)
		rmonth=random.randint(1, 12)
		rday=random.randint(1, 30)
		rhour=random.randint(0, 23)
		rmin=random.randint(0, 59)
		rsec=random.randint(0, 59)
		InvoiceDate=f'{year}-{rmonth}-{rday} {rhour}:{rmin}:{rsec}'
		house_number=random.randint(1, 99999)
		house_street=' '.join(''.join(random.choice('ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz.,') for n in range(random.randint(1,12))) for n in range(random.randint(1,4)))
		#Create Random Address with each word up to 12 characters long, and each street up to 4 words long
		BillingAddress=f'{house_number} {house_street}'
		if ',' in BillingAddress: BillingAddress = f'"{BillingAddress}"'
		#Create Random City Name with each word up to 12 characters long, and each city up to 3 words long
		BillingCity = ' '.join(''.join(random.choice('ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz') for n in range(random.randint(1,12))) for n in range(random.randint(1,3)))
		#Create Random State Name with from 0 to 3 characters long
		BillingState = ''.join(random.choice('ABCDEFGHIJKLMNOPQRSTUVWXYZ') for n in range(random.randint(0,3)))
		#Create Random Country Name with from 3 to 12 characters long and 1 or 2 words
		BillingCountry = ' '.join(''.join(random.choice('ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz') for n in range(random.randint(3,12))) for n in range(random.randint(1,2)))
		#Create Random Postal Code up to 9 characters long including spaces and dashes
		BillingPostalCode = ''.join(random.choice('ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890 -') for n in range(random.randint(0,9)))
		#Create Random Total between 0 and 99999.99
		Total = float(random.randint(1, 99999)) + float(random.randint(0, 99))/100
		if chaos:
			CHAOSMONKEY = random.choice(['CustomerId','InvoiceDate','BillingAddress','BillingCity','BillingState','BillingCountry','BillingPostalCode','Total'])
			if CHAOSMONKEY == 'CustomerId': CustomerId = ''
			elif CHAOSMONKEY == 'InvoiceDate': InvoiceDate = ''
			elif CHAOSMONKEY == 'BillingAddress': BillingAddress = ''
			elif CHAOSMONKEY == 'BillingCity': BillingCity = ''
			elif CHAOSMONKEY == 'BillingState': BillingState = ''
			elif CHAOSMONKEY == 'BillingCountry': BillingCountry = ''
			elif CHAOSMONKEY == 'BillingPostalCode': BillingPostalCode = ''
			elif CHAOSMONKEY == 'Total': Total = ''
		write_to_file+=f'{InvoiceId},{CustomerId},{InvoiceDate},{BillingAddress},{BillingCity},{BillingState},{BillingCountry},{BillingPostalCode},{Total}\n'
		InvoiceId+=1
	filename = f'invoices_{year}.csv'
	fh = open(filename,'w')
	fh.write(write_to_file)
	fh.close()
	return filename
	
def create_json_file(year,num_lines,chaos=False):
	json_data=[]
	InvoiceId=1
	for i in range(num_lines):
		CustomerId=random.randint(1, num_lines)
		rmonth=random.randint(1, 12)
		rday=random.randint(1, 30)
		rhour=random.randint(0, 23)
		rmin=random.randint(0, 59)
		rsec=random.randint(0, 59)
		InvoiceDate=f'{year}-{rmonth}-{rday} {rhour}:{rmin}:{rsec}'
		house_number=random.randint(1, 99999)
		house_street=' '.join(''.join(random.choice('ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz.,') for n in range(random.randint(1,12))) for m in range(random.randint(1,4)))
		#Create Random Address with each word up to 12 characters long, and each street up to 4 words long
		BillingAddress=f'{house_number} {house_street}'
		#if ',' in BillingAddress: BillingAddress = f'"{BillingAddress}"'
		#Create Random City Name with each word up to 12 characters long, and each city up to 3 words long
		BillingCity = ' '.join(''.join(random.choice('ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz') for n in range(random.randint(1,12))) for m in range(random.randint(1,3)))
		#Create Random State Name with from 0 to 3 characters long
		BillingState = ''.join(random.choice('ABCDEFGHIJKLMNOPQRSTUVWXYZ') for n in range(random.randint(0,3)))
		#Create Random Country Name with from 3 to 12 characters long and 1 or 2 words
		BillingCountry = ' '.join(''.join(random.choice('ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz') for n in range(random.randint(3,12))) for m in range(random.randint(1,2)))
		#Create Random Postal Code up to 9 characters long including spaces and dashes
		BillingPostalCode = ''.join(random.choice('ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890 -') for n in range(random.randint(0,9)))
		#Create Random Total between 0 and 99999.99
		Total = float(random.randint(1, 99999)) + float(random.randint(0, 99))/100
		if chaos:
			CHAOSMONKEY = random.choice(['InvoiceId','CustomerId','InvoiceDate','BillingAddress','BillingCity','BillingState','BillingCountry','BillingPostalCode','Total'])
			if CHAOSMONKEY == 'CustomerId': CustomerId = ''
			elif CHAOSMONKEY == 'InvoiceDate': InvoiceDate = ''
			elif CHAOSMONKEY == 'BillingAddress': BillingAddress = ''
			elif CHAOSMONKEY == 'BillingCity': BillingCity = ''
			elif CHAOSMONKEY == 'BillingState': BillingState = ''
			elif CHAOSMONKEY == 'BillingCountry': BillingCountry = ''
			elif CHAOSMONKEY == 'BillingPostalCode': BillingPostalCode = ''
			elif CHAOSMONKEY == 'Total': Total = ''
		tmp_dict = {
		'InvoiceId': InvoiceId,
		'CustomerId': CustomerId,
		'InvoiceDate': InvoiceDate,
		'BillingAddress': BillingAddress,
		'BillingCity': BillingCity,
		'BillingState': BillingState,
		'BillingCountry': BillingCountry,
		'BillingPostalCode': BillingPostalCode,
		'Total': Total
		}
		json_data.append(tmp_dict)
		InvoiceId+=1
	write_to_file = json.dumps(json_data)
	filename = f'invoices_{year}.json'
	fh = open(filename,'w')
	fh.write(write_to_file)
	fh.close()
	return filename
	
#1 minute stress test. Generating input files dynamically.
def test_one():
	connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
	channel = connection.channel()
	channel.queue_declare(queue='producer')
	print('[RUN] TEST 1: STRESS TEST')
	try:
		initial_year = 1000
		runtime = int(time.time())+60
		should_print = False
		while runtime > int(time.time()):
			file_type = random.choice(['JSON','CSV'])
			location=None
			if file_type == 'CSV':
				location = create_csv_file(initial_year,random.randint(1, 999))
			else:
				location = create_csv_file(initial_year,random.randint(1, 999))
			table_name = f'invoices_{initial_year}'
			channel.basic_publish(exchange='', routing_key='producer', body=f'{location},{file_type},{table_name}')
			time.sleep(1) #Needs a while to read the file.
			os.remove(location)
			initial_year+=1
			if should_print and (int(time.time())-runtime) % 5 == 0:
				current_seconds = int(time.time())-runtime
				print(f'\tProgress: {current_seconds}')
				should_print=False
			else: should_print = True
	except Exception as e:
		print(str(e))
		print('[FAILED] TEST 1: STRESS TEST')
		connection.close()
		return
	print('[PASS] TEST 1: STRESS TEST')
	
#Data Missing - run for 10 seconds
def test_two():
	connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
	channel = connection.channel()
	channel.queue_declare(queue='producer')
	print('[RUN] TEST 2: MISSING DATA')
	#try:
	initial_year = 1000
	runtime = int(time.time())+10
	should_print = False
	while runtime > int(time.time()):
		file_type = random.choice(['JSON','CSV'])
		location=None
		if file_type == 'CSV':
			location = create_csv_file(initial_year,random.randint(1, 999),chaos=True)
		else:
			location = create_csv_file(initial_year,random.randint(1, 999),chaos=True)
		table_name = f'invoices_{initial_year}'
		channel.basic_publish(exchange='', routing_key='producer', body=f'{location},{file_type},{table_name}')
		time.sleep(1) #Needs a while to read the file.
		os.remove(location)
		initial_year+=1
		if should_print and (runtime-int(time.time())) % 2 == 0:
			current_seconds = -1*(runtime-int(time.time()))
			print(f'\tProgress: {current_seconds}')
			should_print=False
		else: should_print = True
	'''
	except Exception as e:
		print(str(e))
		print('[FAILED] TEST 2: MISSING DATA')
		connection.close()
		return
	'''
	print('[PASS] TEST 2: MISSING DATA')

#Missing Files
def test_three():
	connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
	channel = connection.channel()
	channel.queue_declare(queue='producer')
	print('[RUN] TEST 3: MISSING FILES')
	try:
		initial_year = 1000
		for i in range(10):
			file_type = random.choice(['JSON','CSV'])
			location=f'missing_file_{i}.xxx'
			table_name = f'invoices_{initial_year}'
			channel.basic_publish(exchange='', routing_key='producer', body=f'{location},{file_type},{table_name}')
			#time.sleep(1) #Needs a while to read the file.
			#os.remove(location)
			initial_year+=1
			print(f'\tProgress: {i}')
	except Exception as e:
		print(str(e))
		print('[FAILED] TEST 3: MISSING FILES')
		connection.close()
		return
	print('[PASS] TEST 3: MISSING FILES')
	
#Invalid Table Name
def test_four():
	connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
	channel = connection.channel()
	channel.queue_declare(queue='producer')
	print('[RUN] TEST 4: INVALID TABLE NAME')
	try:
		initial_year = 1000
		for i in range(10):
			file_type = random.choice(['JSON','CSV'])
			location=None
			if file_type == 'CSV':
				location = create_csv_file(initial_year,random.randint(1, 999))
			else:
				location = create_csv_file(initial_year,random.randint(1, 999))
			table_name = f'!@#$%^&*()_{initial_year}'
			channel.basic_publish(exchange='', routing_key='producer', body=f'{location},{file_type},{table_name}')
			time.sleep(1) #Needs a while to read the file.
			os.remove(location)
			initial_year+=1
			print(f'\tProgress: {i}')
	except Exception as e:
		print(str(e))
		print('[FAILED] TEST 4: INVALID TABLE NAME')
		connection.close()
		return
	print('[PASS] TEST 4: INVALID TABLE NAME')
	
#Invalid File Type
def test_five():
	connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
	channel = connection.channel()
	channel.queue_declare(queue='producer')
	print('[RUN] TEST 5: INVALID FILE TYPE')
	try:
		initial_year = 1000
		for i in range(10):
			file_type = random.choice(['JSON','CSV'])
			location=None
			if file_type == 'CSV':
				location = create_csv_file(initial_year,random.randint(1, 999))
			else:
				location = create_csv_file(initial_year,random.randint(1, 999))
			table_name = f'invoices_{initial_year}'
			file_type = 'ZZZ'
			channel.basic_publish(exchange='', routing_key='producer', body=f'{location},{file_type},{table_name}')
			time.sleep(1) #Needs a while to read the file.
			os.remove(location)
			initial_year+=1
			print(f'\tProgress: {i}')
	except Exception as e:
		print(str(e))
		print('[FAILED] TEST 5: INVALID FILE TYPE')
		connection.close()
		return
	print('[PASS] TEST 5: INVALID FILE TYPE')

if not RUN_TESTS:
	main()
else:
	test_five()
	test_four()
	test_three()
	test_two()
	test_one()