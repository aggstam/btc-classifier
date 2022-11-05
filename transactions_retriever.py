# -------------------------------------------------------------
#
# This script generates the execution dataset for the analyzer.py script.
# A random address sample is retrieved from the original files and their
# transactions ids are retrieved from the DB, to create the execution dataset.
#
# Original dataset: https://github.com/Maru92/EntityAddressBitcoin
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

import logging, time, csv, random
import mysql.connector as mysql

# Execution configuration.
logging.basicConfig(format='%(asctime)s.%(msecs)07d: %(message)s', level=logging.INFO, datefmt='%Y-%m-%d %H:%M:%S')

# Original dataset files configuration.
# file = ['file_path', address_position, address_limit]
EXCHANGES_ADDRESSES_FILE = ['Addresses/Exchanges_full_detailed.csv', 5, 10]
GAMBLING_ADDRESSES_FILE = ['Addresses/Gambling_full_detailed.csv', 4, 10]
HISTORIC_ADDRESSES_FILE = ['Addresses/Historic_full_detailed.csv', 4, 10]
MALICIOUS_ADDRESSES_FILE = ['Addresses/Malicious_addresses.csv', 0, 300]
MINING_ADDRESSES_FILE = ['Addresses/Mining_full_detailed.csv', 4, 10]
SERVICES_ADDRESSES_FILE = ['Addresses/Services_full_detailed.csv', 4, 10]

# Database queries used to retrieve the dataset.
# Using this queries, all transactions related to given address list are retrieved.
TXIN_QUERY = 'SELECT DISTINCT(t1.txid) FROM btc.tx t1 JOIN btc.txin t2 ON (t1.txid = t2.consume_txid) JOIN btc.txout t3 ON (t2.output_txid = t3.output_txid AND t2.vout = t3.vout) WHERE t1.timestamp < \'2018-04-01\' and t3.address IN '
TXOUT_QUERY = 'SELECT DISTINCT(t1.txid) FROM btc.tx t1 JOIN btc.txout t2 ON (t1.txid = t2.output_txid) WHERE t1.timestamp < \'2018-04-01\' and t2.address IN '

# Generated file paths.
TRANSACTIONS_CSV_FILE = 'Generated_Files/transactions.csv'
EXCHANGES_ADDRESSES_CSV_FILE = 'Generated_Files/exchanges_addresses.csv'
GAMBLING_ADDRESSES_CSV_FILE = 'Generated_Files/gambling_addresses.csv'
HISTORIC_ADDRESSES_CSV_FILE = 'Generated_Files/historic_addresses.csv'
MALICIOUS_ADDRESSES_CSV_FILE = 'Generated_Files/malicious_addresses.csv'
MINING_ADDRESSES_CSV_FILE = 'Generated_Files/mining_addresses.csv'
SERVICES_ADDRESSES_CSV_FILE = 'Generated_Files/services_addresses.csv'

# Parses a csv file, using the file configuration to identify address position and address limit.
def read_csv_file(file):
	logging.info('Retrieving addresses from csv: ' + file[0])
	logging.info('Addresses limit: ' + str(file[2]))
	addresses = set()
	with open(file[0]) as csv_file:
		csv_reader = csv.reader(csv_file)		
		header = next(csv_reader)
		for row in csv_reader:			
			if row[file[1]] not in addresses:
				addresses.add(row[file[1]])
	logging.info('Addresses found: ' + str(len(addresses)))
	random_addresses = random.sample(addresses, file[2]) if len(addresses) > file[2] else list(addresses)
	return random_addresses, addresses

# Initializes a connection with the MySQL Database.
def init_database():
	logging.info('Initializing Database connection...')
	host = 'localhost'
	user = 'root'
	password = 'root'
	database = 'btc'
	db = mysql.connect(host=host, user=user, password=password, database=database)
	logging.info('Database connection initialized!')
	return db;

# Closes an active connection to the Database.
# "RESTART" command is used as to reset DB cache for memory optimization.
def close_database(db, cursor):
	logging.info('Closing Database connection...')
	if db is not None and db.is_connected():
		cursor.execute('RESTART;')
		db.close()
	logging.info('Database connection closed!')

# Executing given query and appending retrieved data to transactions set.
def execute_query(transactions, cursor, query, label):	
	logging.info('Executing: ' + label)
	query_time = time.time()
	cursor.execute(query)
	count = 0
	for result in cursor:		
		if result[0] not in transactions:
			transactions.add(result[0])
		count += 1
	logging.info('Finished executing query (' + str(count) + ' records) ! Elapsed time: ' + time.strftime('%H:%M:%S', time.gmtime(time.time() - query_time)))

# Generates an output CSV file.
def generate_csv_file(csv_file, header, records):
	logging.info('Generating file: ' + csv_file)
	with open(csv_file,'w') as file:
		file.write(header + '\n')
		for record in records:
			file.write(record + '\n')
	logging.info('File generated!')

#####################################################

# Script execution order:
#	1. Parse original dataset files and sample random address records.
#	2. Retrieve all transactions of the address sample from the Database.
#	3. Generating a CSV file containing the retrieved transactions.
#	4. Generate a CSV file containing the address list for each original dataset file to a more usable format.

total_time = time.time()
logging.info('Retrieving address records...')
random_exchanges_addresses, exchanges_addresses = read_csv_file(EXCHANGES_ADDRESSES_FILE)
random_gambling_addresses, gambling_addresses = read_csv_file(GAMBLING_ADDRESSES_FILE)
random_historic_addresses, historic_addresses = read_csv_file(HISTORIC_ADDRESSES_FILE)
random_malicious_addresses, malicious_addresses = read_csv_file(MALICIOUS_ADDRESSES_FILE)
random_mining_addresses, mining_addresses = read_csv_file(MINING_ADDRESSES_FILE)
random_services_addresses, services_addresses = read_csv_file(SERVICES_ADDRESSES_FILE)

logging.info('Retrieving transaction records...')
db = init_database()
cursor = db.cursor()
transactions = set()
execute_query(transactions, cursor, TXIN_QUERY + str(random_exchanges_addresses).replace('[','(').replace(']',')'), 'TXIN_QUERY for random_exchanges_addresses...')
execute_query(transactions, cursor, TXOUT_QUERY + str(random_exchanges_addresses).replace('[','(').replace(']',')'), 'TXOUT_QUERY for random_exchanges_addresses...')
execute_query(transactions, cursor, TXIN_QUERY + str(random_gambling_addresses).replace('[','(').replace(']',')'), 'TXIN_QUERY for random_gambling_addresses...')
execute_query(transactions, cursor, TXOUT_QUERY + str(random_gambling_addresses).replace('[','(').replace(']',')'), 'TXOUT_QUERY for random_gambling_addresses...')
execute_query(transactions, cursor, TXIN_QUERY + str(random_historic_addresses).replace('[','(').replace(']',')'), 'TXIN_QUERY for random_historic_addresses...')
execute_query(transactions, cursor, TXOUT_QUERY + str(random_historic_addresses).replace('[','(').replace(']',')'), 'TXOUT_QUERY for random_historic_addresses...')
execute_query(transactions, cursor, TXIN_QUERY + str(random_malicious_addresses).replace('[','(').replace(']',')'), 'TXIN_QUERY for random_malicious_addresses...')
execute_query(transactions, cursor, TXOUT_QUERY + str(random_malicious_addresses).replace('[','(').replace(']',')'), 'TXOUT_QUERY for random_malicious_addresses...')
execute_query(transactions, cursor, TXIN_QUERY + str(random_mining_addresses).replace('[','(').replace(']',')'), 'TXIN_QUERY for random_mining_addresses...')
execute_query(transactions, cursor, TXOUT_QUERY + str(random_mining_addresses).replace('[','(').replace(']',')'), 'TXOUT_QUERY for random_mining_addresses...')
execute_query(transactions, cursor, TXIN_QUERY + str(random_services_addresses).replace('[','(').replace(']',')'), 'TXIN_QUERY for random_services_addresses...')
execute_query(transactions, cursor, TXOUT_QUERY + str(random_services_addresses).replace('[','(').replace(']',')'), 'TXOUT_QUERY for random_services_addresses...')
close_database(db, cursor);
logging.info('Finished retrieving transaction records! Total transactions: ' + str(len(transactions)))

generate_csv_file(TRANSACTIONS_CSV_FILE, 'txid', transactions)
generate_csv_file(EXCHANGES_ADDRESSES_CSV_FILE, 'address', exchanges_addresses)
generate_csv_file(GAMBLING_ADDRESSES_CSV_FILE, 'address', gambling_addresses)
generate_csv_file(HISTORIC_ADDRESSES_CSV_FILE, 'address', historic_addresses)
generate_csv_file(MALICIOUS_ADDRESSES_CSV_FILE, 'address', malicious_addresses)
generate_csv_file(MINING_ADDRESSES_CSV_FILE, 'address', mining_addresses)
generate_csv_file(SERVICES_ADDRESSES_CSV_FILE, 'address', services_addresses)
logging.info('Total Execution time: ' + time.strftime('%H:%M:%S', time.gmtime(time.time() - total_time)))
