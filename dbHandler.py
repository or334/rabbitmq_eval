import sqlite3
import threading
#SQL Lite only allows a single thread to write to the database at any time.
DBLock = threading.Condition()

class dbHandler:
	def __init__(self,dbPath):
		self.path=dbPath
		self.conn = None

	def getPath(self):
		return self.path

	def connect(self):
		try:
			self.conn = sqlite3.connect(self.path)
			return True
		except :
			print ('connection error')
			return False

	def disconnect(self):
		if self.conn:
			self.conn.close()
			#print("disconnected from database")

	def select(self, query):
		cur = self.conn.cursor()
		cur.execute(query)
		rs = cur.fetchall()
		if rs == None: return None
		if len(rs) == 0: rs = None
		elif len(rs) == 1: rs = rs[0]
		return rs
		
	def get_new_index(self, table_name):
		index = -1
		try:
			if self.connect():
				index = self.select(f'SELECT CASE WHEN MAX(id) IS NULL THEN 0 ELSE (MAX(id) + 1) END FROM {table_name}')
				self.disconnect()
				if type(index) == tuple: index = index[0]
		except Exception as e:
			print(str(e))
			print(f'[ERROR] Failed to get new index for {table_name}')
			pass
		return index
		
	def create_table(self, table_name, key_names, key_types):
		HEADERS = ['ID		INT		PRIMARY KEY		NOT NULL']
		index = 0
		for column_name in key_names:
			TYPE = key_types[index]
			index+=1
			HEADERS.append(f'{column_name}		{TYPE}		NULL')
		HEADERS = ','.join(x for x in HEADERS)
		try:
			if self.connect():
				DBLock.acquire()
				self.conn.execute(f'CREATE TABLE IF NOT EXISTS {table_name} ({HEADERS});')
				self.conn.commit()
				DBLock.notify_all()
				DBLock.release()
				self.disconnect()
			else:
				print(f'[ERROR] create_table not connected. {table_name}')
				return False
		except Exception as e:
			print(str(e))
			print(f'[ERROR] create_table exception {table_name}')
			print(f'[DEBUG] CREATE TABLE IF NOT EXISTS {table_name} ({HEADERS});')
			return False #Fail
		#print(f'[SUCCESS] create_table {table_name}')
		return True #Success
		
	def insert_into_table(self, table_name, key_names, key_types, rows):
		#print(key_names)
		#print(key_types)
		initial_index = self.get_new_index(table_name)
		if initial_index == -1: return False #Fail
		HEADERS = ['ID']
		for key in key_names:
			HEADERS.append(key)
		HEADERS = ','.join(x for x in HEADERS)
		ROWS=[]
		for values in rows:
			ONE_ROW=[str(initial_index)]
			tmp_index = 0
			for v in values:
				if key_types[tmp_index] == 'TEXT':
					ONE_ROW.append(f"'{v}'")
				else:
					ONE_ROW.append(f"{v}")
				tmp_index+=1
			tmp_row = ','.join(x for x in ONE_ROW)
			ROWS.append(f'({tmp_row})')
			initial_index+=1
		ROWS = ','.join(x for x in ROWS)
		try:
			if self.connect():
				DBLock.acquire()
				self.conn.execute(f'INSERT INTO {table_name} ({HEADERS}) VALUES {ROWS};')
				self.conn.commit()
				DBLock.notify_all()
				DBLock.release()
				self.disconnect()
			else:
				print(f'[ERROR] insert_into_table not connected. {table_name}')
				return False
		except Exception as e:
			print(str(e))
			print(f'[ERROR] insert_into_table exception {table_name}')
			print(f'[DEBUG] INSERT INTO {table_name} ({HEADERS}) VALUES {ROWS};')
			return False #Fail
		#print(f'[SUCCESS] insert_into_table {table_name}')
		return True #Success
		





