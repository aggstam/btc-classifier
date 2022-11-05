# -------------------------------------------------------------
#
# This script parses the output files of parser.py script and 
# generates the MySQL Database records.
#
# Author: Aggelos Stamatiou, April 2021
#
# This source code is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This software is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this source code. If not, see <http://www.gnu.org/licenses/>.
# --------------------------------------------------------------

import time
import csv
import mysql.connector as mysql

# Class mapping `tx` DB records.
class TX:
	def __init__(self, txid, timestamp):
		self.txid = txid
		self.timestamp = timestamp
		
	def __str__(self):
		return 'TX=[txid={0}, timestamp={1}]'.format(self.txid, self.timestamp)
		
	def insert_record(self, db):
		cursor = db.cursor()
		cursor.execute('INSERT INTO tx VALUES(\'{0}\', \'{1}\')'.format(self.txid, self.timestamp))

# Class mapping `txin` DB records.		
class TXIN:
	def __init__(self, output_txid, consume_txid, vout):
		self.output_txid = output_txid
		self.consume_txid = consume_txid
		self.vout = vout
		
	def __str__(self):
		return 'TXIN=[output_txid={0}, consume_txid={1}, vout={2}]'.format(self.output_txid, self.consume_txid, self.vout)
		
	def insert_record(self, db):
		cursor = db.cursor()
		cursor.execute('INSERT INTO txin VALUES(\'{0}\', \'{1}\', \'{2}\')'.format(self.output_txid, self.consume_txid, self.vout))

# Class mapping `txout` DB records.		
class TXOUT:
	def __init__(self, output_txid, vout, address, value):
		self.output_txid = output_txid
		self.vout = vout
		self.address = address
		self.value = value
		
	def __str__(self):
		return 'TXOUT=[output_txid={0}, vout={1}, address={2}, value={3}]'.format(self.output_txid, self.vout, self.address, self.value)
		
	def insert_record(self, db):
		cursor = db.cursor()
		cursor.execute('INSERT INTO txout VALUES(\'{0}\', \'{1}\', \'{2}\', \'{3}\')'.format(self.output_txid, self.vout, self.address, self.value))

# Initializes a connection with the MySQL Database and creates the DB schema, in case it is not present.
def init_database():
	host = 'localhost'
	user = 'root'
	password = 'root'
	database = 'btc'
	db = mysql.connect(host=host, user=user, password=password)
	cursor = db.cursor()
	cursor.execute('CREATE DATABASE IF NOT EXISTS ' + database)
	db = mysql.connect(host=host, user=user, password=password, database=database)
	cursor = db.cursor()
	cursor.execute('CREATE TABLE IF NOT EXISTS tx (txid VARCHAR(255) NOT NULL, timestamp DATETIME NOT NULL)')
	cursor.execute('CREATE TABLE IF NOT EXISTS txin (output_txid VARCHAR(255) NOT NULL, consume_txid VARCHAR(255) NOT NULL, vout BIGINT NOT NULL)')
	cursor.execute('CREATE TABLE IF NOT EXISTS txout (output_txid VARCHAR(255) NOT NULL, vout BIGINT NOT NULL, address VARCHAR(255) NOT NULL, value DOUBLE NOT NULL)')	
	return db;

# Closes an active connection to the Database.
def close_database(db):
	if db is not None and db.is_connected():
		db.close()	

# For a given file:
#	1. Parse file and create the tx, txin and txout records to be created.
#	2. Insert all parsed records in database.
# Batching commit has been implemented for optimization.
def parse_file(db, file):
	start_time = time.time()
	print ('Start reading file ' + str(file) + ' at: ' + time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(start_time)))
	with open(file, newline='') as f:
		reader = csv.reader(f)
		records = list(reader)

	tx_list = []
	txin_list = []
	txout_list = []
	for record in records:
		if record[0] == 'tx':
			tx = TX(record[1], record[2].replace(';', ''))
			tx_list.append(tx)
		elif record[0] == 'txin':
			txin = TXIN(record[1], record[2], record[3].replace(';', ''))
			txin_list.append(txin)
		else:
			txout = TXOUT(record[1], record[2], record[3], record[4].replace(';', ''))
			txout_list.append(txout)
	
	commit_counter = 0;
	for tx in tx_list:
		tx.insert_record(db)
		if (commit_counter == 10000):
			db.commit();
			commit_counter = 0
		else:
			commit_counter += 1
	db.commit()

	for txin in txin_list:
		txin.insert_record(db)
		if (commit_counter == 10000):
			db.commit();
			commit_counter = 0
		else:
			commit_counter += 1
	db.commit()
		
	for txout in txout_list:
		txout.insert_record(db)
		if (commit_counter == 10000):
			db.commit();
			commit_counter = 0
		else:
			commit_counter += 1
	db.commit()

	print ('Finished reading file ' + str(file) + '! Elapsed time: ' + time.strftime('%H:%M:%S', time.gmtime(time.time() - start_time)))

#####################################################

# Script execution order:
#	1. Initialize DB connection.
#	2. Parse files with index in specific range(implemented for batch processing).
#	3. Close DB connection.
	
dir = 'parser_output/'
start_index = 2364
end_index = 2400
db = init_database()
for x in range(start_index, end_index):
	file = dir + 'blk' + f'{x:05d}' + '.txt'
	parse_file(db, file)
close_database(db)
