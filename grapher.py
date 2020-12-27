#!/usr/bin/env python
import pika, sys, os
import matplotlib
import matplotlib.pyplot as plt
import matplotlib.animation as animation
from matplotlib import style
import dbHandler
import threading

AX1_DATA=[['2020-10','2020-11','2020-12'],[5,2,8]]
AX2_DATA=[['2020-10','2020-11','2020-12'],[2,8,5]]
_graph_event = threading.Event()
		
class GraphConsumer(threading.Thread):
	
	def __init__(self, gsi=-1):
		threading.Thread.__init__(self)
		connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
		self.channel = connection.channel()
		self.channel.queue_declare(queue='grapher')
		self.SQLDB = dbHandler.dbHandler('test.db')
	
	#	Total sales per month each year
	def get_total_sales(self):
		if not self.SQLDB.connect(): return None
		result = self.SQLDB.select("SELECT name FROM sqlite_master WHERE type ='table' AND name NOT LIKE 'sqlite_%';")
		#print(result)
		if type(result) == tuple: result = [result]
		TOTAL_SALES={}
		for row in result:
			table_name = row[0]
			#print(f'[DEBUG] SELECT INVOICEDATE, TOTAL FROM {table_name};')
			big_data = self.SQLDB.select(f'SELECT INVOICEDATE, TOTAL FROM {table_name};')
			#print(big_data)
			for data in big_data:
				INVOICEDATE=data[0]
				TOTAL=data[1]
				if TOTAL == None: continue
				month = INVOICEDATE[:7] #2020-01
				if month not in TOTAL_SALES:
					TOTAL_SALES[month]=0
				if type(TOTAL) != int:
					try:
						TOTAL = int(TOTAL)
					except:
						print('[ERROR] TOTAL is not type int')
						continue
				TOTAL_SALES[month]+=TOTAL
		self.SQLDB.disconnect()
		xValues = []
		yValues = []
		for key in sorted(TOTAL_SALES.keys()):
			xValues.append(key)
			yValues.append(TOTAL_SALES[key])
		return [xValues,yValues]
	
	#	Number of active customers in the same month
	def get_active_customers(self):
		if not self.SQLDB.connect(): return None
		result = self.SQLDB.select("SELECT name FROM sqlite_master WHERE type ='table' AND name NOT LIKE 'sqlite_%';")
		#print(result)
		if type(result) == tuple: result = [result]
		TOTAL_ACTIVE={}
		for row in result:
			table_name = row[0]
			#print(f'[DEBUG] SELECT INVOICEDATE, CUSTOMERID FROM {table_name};')
			big_data = self.SQLDB.select(f'SELECT INVOICEDATE, CUSTOMERID FROM {table_name};')
			#print(big_data)
			for data in big_data:
				INVOICEDATE=data[0]
				CUSTOMERID=data[1]
				month = INVOICEDATE[:7] #2020-01
				if month not in TOTAL_ACTIVE:
					TOTAL_ACTIVE[month]=[]
				if CUSTOMERID not in TOTAL_ACTIVE[month]:
					TOTAL_ACTIVE[month].append(CUSTOMERID)
		self.SQLDB.disconnect()
		xValues = []
		yValues = []
		for key in sorted(TOTAL_ACTIVE.keys()):
			xValues.append(key)
			yValues.append(len(TOTAL_ACTIVE[key]))
		return [xValues,yValues]
		
	def grapher_cb(self,ch, meth, prop, bod):
		#Create the base graph for:
		#	Total sales per month each year
		#	Number of active customers in the same month
		#	- active customer per month is a customer who has made at least one purchase
		
		bod = json_string=bod.decode("utf-8")
		#print(f'\tgrapher_cb received {bod}')
		global AX1_DATA,AX2_DATA,_graph_event
		AX1_DATA=self.get_total_sales()
		AX2_DATA=self.get_active_customers()
		_graph_event.set()
		#print('[DEBUG] Updating Graph...')
	
	def stop(self):
		if self.channel.is_open:
			self.channel.stop_consuming()
		
	def run(self):
		self.channel.basic_consume(queue='grapher', on_message_callback=self.grapher_cb, auto_ack=True)
		print('Waiting for consumer...')
		self.channel.start_consuming()

def animate(i):
	#print('[DEBUG] animate...')
	global _graph_event
	if not _graph_event.is_set(): return
	global ax1,ax2,AX1_DATA,AX2_DATA
	ax1.clear()
	ax2.clear()
	
	plt.setp(ax1, ylabel='Sales')
	ax1.title.set_text('Total Sales Per Month')
	ax1.plot(AX1_DATA[0],AX1_DATA[1], color = 'green', label = 'Sales')
	for tick in ax1.get_xticklabels():
		tick.set_rotation(90)
		
	ax2.title.set_text('Active Customers Per Month')
	plt.setp(ax2, ylabel='Customers')
	ax2.plot(AX2_DATA[0],AX2_DATA[1], color = 'blue', label = 'Customers')
	for tick in ax2.get_xticklabels():
		tick.set_rotation(90)
	_graph_event.clear()
	print('[SUCCESS] Updated Graph...')

#plt.ion()
gc = GraphConsumer()
gc.start()
#print('[DEBUG] Started GraphConsumer')
style.use('fivethirtyeight')
matplotlib.rc('xtick', labelsize=8) 
matplotlib.rc('ytick', labelsize=8) 
fig = plt.figure(figsize=(15,8))
ax1 = fig.add_subplot(211)
ax2 = fig.add_subplot(212)
ani = animation.FuncAnimation(fig, animate, interval=500)
plt.show()
gc.stop()

