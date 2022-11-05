# -------------------------------------------------------------
#
# This script performs an unsupervised Machine Learning task,
# using DeepInfomax and GCN algorithms for node representation learning
# and Logistic regression for the classification of each node,
# on the temporal network graph for the Bitcoin transactions dataset.
# StellarGraph libraries are used for the Machine Learning tasks.
# On each execution, a dedicated folder is created containing all result files.
#
# StellarGraph: https://github.com/stellargraph/stellargraph
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

import os, logging, time, csv
from enum import Enum
from datetime import datetime
import mysql.connector as mysql
import networkx as nx
import pandas as pd
import numpy as np
from matplotlib import pyplot as plt
from collections import Counter

import stellargraph as sg
import tensorflow as tf
from stellargraph import StellarDiGraph
from stellargraph.utils import plot_history
from stellargraph.mapper import CorruptedGenerator, FullBatchNodeGenerator
from stellargraph.layer import GCN, DeepGraphInfomax
from tensorflow.keras import layers, optimizers, losses, Model
from tensorflow.keras.optimizers import Adam
from tensorflow.keras.callbacks import EarlyStopping
from sklearn import model_selection
from sklearn.linear_model import LogisticRegression

# Execution configuration.
tf.compat.v1.disable_eager_execution() # Decreases memory consuptions due to a tensorflow library bug.
logging.basicConfig(format='%(asctime)s.%(msecs)06d: %(message)s', level=logging.INFO, datefmt='%Y-%m-%d %H:%M:%S')

# Execution paths.
OUTPUT_FOLDER = 'Executions/'
TRANSACTIONS_CSV_FILE = 'Generated_Files/transactions.csv'
EXCHANGES_ADDRESSES_CSV_FILE = 'Generated_Files/exchanges_addresses.csv'
GAMBLING_ADDRESSES_CSV_FILE = 'Generated_Files/gambling_addresses.csv'
HISTORIC_ADDRESSES_CSV_FILE = 'Generated_Files/historic_addresses.csv'
MALICIOUS_ADDRESSES_CSV_FILE = 'Generated_Files/malicious_addresses.csv'
MINING_ADDRESSES_CSV_FILE = 'Generated_Files/mining_addresses.csv'
SERVICES_ADDRESSES_CSV_FILE = 'Generated_Files/services_addresses.csv'

# Database queries used to retrieve the dataset.
TXIN_QUERY = 'SELECT t3.address, t1.txid, t1.timestamp, t3.value FROM btc.tx t1 JOIN btc.txin t2 ON (t1.txid = t2.consume_txid) JOIN btc.txout t3 ON (t2.output_txid = t3.output_txid AND t2.vout = t3.vout) WHERE t1.txid in '
TXOUT_QUERY = 'SELECT t1.txid, t2.address, t1.timestamp, t2.value FROM btc.tx t1 JOIN btc.txout t2 ON (t1.txid = t2.output_txid) WHERE t1.txid in '

# Machine Learning execution parameters.
FOLDS = 10
EPOCHS = 500

# Utility classes used for mapping node types and flags to integers.
class Node_Type(Enum):
	ADDRESS = 0
	TRANSACTION = 1
class Node_Flag(Enum):
	TRANSACTION = 0
	UNKNOWN = 1
	EXCHANGES = 2
	GAMBLING = 3
	HISTORIC = 4
	MALICIOUS = 5
	MINING = 6
	SERVICES = 7

# Create the outputs folder.
def create_output_folder():
	logging.info('Creating outputs folder...')
	output_folder = OUTPUT_FOLDER + datetime.now().strftime("%Y_%m_%d_%H_%M_%S") + '/'
	os.mkdir(output_folder)
	logging.info('Outputs folder ' + output_folder + ' created.')
	return output_folder

# Parses a csv file.
def read_csv_file(file):
	logging.info('Retrieving records from csv: ' + file)
	records = set()
	with open(file) as csv_file:
		csv_reader = csv.reader(csv_file)		
		header = next(csv_reader)
		for row in csv_reader:			
			if row[0] not in records:
				records.add(row[0])
	logging.info('Records found: ' + str(len(records)))
	#logging.info('Records: ' + str(records))
	return records

# Parse each execution file and build the execution records dictionary, used for labeling graph nodes.
def retrieve_execution_records():
	logging.info('Retrieving execution records...')
	transactions = read_csv_file(TRANSACTIONS_CSV_FILE)
	exchanges_addresses = read_csv_file(EXCHANGES_ADDRESSES_CSV_FILE)
	gambling_addresses = read_csv_file(GAMBLING_ADDRESSES_CSV_FILE)
	historic_addresses = read_csv_file(HISTORIC_ADDRESSES_CSV_FILE)
	malicious_addresses = read_csv_file(MALICIOUS_ADDRESSES_CSV_FILE)
	mining_addresses = read_csv_file(MINING_ADDRESSES_CSV_FILE)
	services_addresses = read_csv_file(SERVICES_ADDRESSES_CSV_FILE)
	execution_records_dict = {'transactions':transactions, 'exchanges_addresses':exchanges_addresses, 'gambling_addresses':gambling_addresses, 'historic_addresses':historic_addresses, 'malicious_addresses':malicious_addresses, 'mining_addresses':mining_addresses, 'services_addresses':services_addresses}
	logging.info('Execution records retrieved!')
	return execution_records_dict

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

# For a given address, identify its flag from the execution records dictionary.
def retrieve_address_flag(execution_records_dict, address):
	flag = Node_Flag.UNKNOWN.value
	if address in execution_records_dict['exchanges_addresses']:
		flag = Node_Flag.EXCHANGES.value
	elif address in execution_records_dict['gambling_addresses']:
		flag = Node_Flag.GAMBLING.value
	elif address in execution_records_dict['historic_addresses']:
		flag = Node_Flag.HISTORIC.value
	elif address in execution_records_dict['malicious_addresses']:
		flag = Node_Flag.MALICIOUS.value
	elif address in execution_records_dict['mining_addresses']:
		flag = Node_Flag.MINING.value
	elif address in execution_records_dict['services_addresses']:
		flag = Node_Flag.SERVICES.value
	return flag

# Execute TXIN_QUERY and convert retrieved data to networkx graph nodes.
def execute_txin_query(cursor, execution_records_dict, graph, addresses, transactions):
	logging.info('Fetching TXIN records and converting to graph data...')
	txin_query = TXIN_QUERY + str(execution_records_dict['transactions']).replace('{','(').replace('}',')')
	cursor.execute(txin_query)
	count = 0
	for result in cursor:		
		if result[0] not in addresses:
			flag = retrieve_address_flag(execution_records_dict, result[0])
			graph.add_node(result[0], type=Node_Type.ADDRESS.value, flag=flag)
			addresses.add(result[0])
		if result[1] not in transactions:
			graph.add_node(result[1], type=Node_Type.TRANSACTION.value, flag=Node_Flag.TRANSACTION.value)
			transactions.add(result[1])
		graph.add_edge(result[0], result[1], weight=result[3], timestamp=datetime.timestamp(result[2]))
		count += 1
	logging.info('Finished TXIN records retriaval (' + str(count) + ') and conversion!')

# Execute TXOUT_QUERY and convert retrieved data to networkx graph nodes.	
def execute_txout_query(cursor, execution_records_dict, graph, addresses, transactions):
	logging.info('Fetching TXOUT records and converting to graph data...')
	txout_query = TXOUT_QUERY + str(execution_records_dict['transactions']).replace('{','(').replace('}',')')
	cursor.execute(txout_query)
	count = 0
	for result in cursor:		
		if result[0] not in transactions:
			graph.add_node(result[0], type=Node_Type.TRANSACTION.value, flag=Node_Flag.TRANSACTION.value)
			transactions.add(result[0])
		if result[1] not in addresses:
			flag = retrieve_address_flag(execution_records_dict, result[1])
			graph.add_node(result[1], type=Node_Type.ADDRESS.value, flag=flag)
			addresses.add(result[1])
		graph.add_edge(result[0], result[1], weight=result[3], timestamp=datetime.timestamp(result[2]))
		count += 1
	logging.info('Finished TXOUT records retriaval (' + str(count) + ') and conversion!')

# A networkx graph is created using the DB queries retrieved records,
# graphml file is extracted for further visualization in external tools, 
# and then the graph is converted to a StellarGraph object, used by the ML task.
def generate_graph(execution_records_dict):
	logging.info('Generating graph...')
	db = init_database()
	cursor = db.cursor()
	graph = nx.DiGraph()
	addresses = set()
	transactions = set()
	execute_txin_query(cursor, execution_records_dict, graph, addresses, transactions)
	execute_txout_query(cursor, execution_records_dict, graph, addresses, transactions)
	close_database(db, cursor)
	logging.info('Generating graph file...')
	nx.write_graphml_xml(graph, OUTPUT_FOLDER + 'graph.graphml')  
	logging.info('Graph file gemerated! Generating StellarGraph object...')
	graph_dict = dict(graph.nodes())
	edge_matrix = nx.to_pandas_edgelist(graph)
	stellar_graph = StellarDiGraph(pd.DataFrame.from_dict(graph_dict, orient='index'), edge_matrix, dtype='float32')
	logging.info(stellar_graph.info())
	node_flags = pd.DataFrame.from_dict(graph.nodes, orient='index')['flag']
	logging.info(Counter(node_flags))
	logging.info('StellarGraph generated!')
	return stellar_graph, node_flags

# Given a StellarGraph object, DeepInfomax + GCN node repsentation learing(features) task is performed.
def deep_graph_infomax(stellar_graph):
	logging.info('Generating Deep Graph Infomax model for node represation learning...')

	logging.info('Creating data generators...')
	fullbatch_generator = FullBatchNodeGenerator(stellar_graph, sparse=False)
	corrupted_generator = CorruptedGenerator(fullbatch_generator)
	gen = corrupted_generator.flow(stellar_graph.nodes())
	logging.info('Data generators created!')
	
	logging.info('Creating DeepGraphInfomax + GCN model...')
	gcn_model = GCN(layer_sizes=[128], activations=["relu"], generator=fullbatch_generator)
	infomax = DeepGraphInfomax(gcn_model, corrupted_generator)
	x_in, x_out = infomax.in_out_tensors()
	model = Model(inputs=x_in, outputs=x_out)
	model.compile(loss=tf.nn.sigmoid_cross_entropy_with_logits, optimizer=Adam(lr=1e-3))
	logging.info('DeepGraphInfomax + GCN model created!')
	
	logging.info('Training generated model to learn node representations...')
	es = EarlyStopping(monitor="loss", min_delta=0, patience=20)
	history = model.fit(gen, epochs=EPOCHS, verbose=0, callbacks=[es])
	logging.info('Generated model trained!')
	
	logging.info('Generating history plot file...')
	plot_history(history)
	plt.savefig(OUTPUT_FOLDER + 'history.png')
	logging.info('History plot file gemerated!')
	
	logging.info('Extracting Embeddings...')
	x_emb_in, x_emb_out = gcn_model.in_out_tensors()
	x_out = tf.squeeze(x_emb_out, axis=0)
	emb_model = Model(inputs=x_emb_in, outputs=x_out)
	logging.info('Embeddings extracted!')

	logging.info('Deep Graph Infomax model generated!')
	return fullbatch_generator, emb_model

# Given the features model of Graph, a classification task is performed using Logistic Regression.
def train_and_avaluate(generator, model, train_subjects, test_subjects):
	logging.info('Training classifier and performing predictions using Logistic Regression...')
	train_gen = generator.flow(train_subjects.index)
	test_gen = generator.flow(test_subjects.index)
	train_embeddings = model.predict(train_gen)
	test_embeddings = model.predict(test_gen)	
	lr = LogisticRegression(multi_class="auto", solver="lbfgs", max_iter=500)
	lr.fit(train_embeddings, train_subjects)
	y_pred = lr.predict(test_embeddings)
	gcn_acc = (y_pred == test_subjects).mean()
	logging.info('Test classification accuracy: ' + str(gcn_acc))	
	return gcn_acc, y_pred

# Given a StellarGraph object and its node flags set:
# 	1. Create the node represation model.
#	2. Genarate k-folds for evaluation
#	3. Train and evaluate each fold.
#	4. Extract each fold predictions to a file.
#	5. Extract execution statistics to a file.
#	6. Extract best fold predictions to a file.
def execute_graph_ML(stellar_graph, node_flags):
	logging.info('Executing graph Machine Learning using StellarGraph Deep Graph Infomax + GCN algorithm...')
	
	# Generating Deep Graph Infomax model.
	generator, model = deep_graph_infomax(stellar_graph)
	
	# Generating k-folds.
	logging.info('Generating ' + str(FOLDS) + ' folds...')
	folds = model_selection.StratifiedShuffleSplit(n_splits=FOLDS, test_size=0.3, random_state=42).split(node_flags, node_flags)
	logging.info('Folds generated!')
	
	# Train and evaluate each fold.
	accuracies = []
	best_fold = 0
	best_accuracy = 0
	best_fold_predictions = None
	best_fold_test_subjects = None
	for i, (train_subjects, test_subjects) in enumerate(folds):
		logging.info('Training and evaluating on fold ' + str(i) + '...')
		acc, pred = train_and_avaluate(generator, model, node_flags[train_subjects], node_flags[test_subjects])
		# Extract fold predictions to a file.
		df = pd.DataFrame({"Predicted": pred, "True": node_flags[test_subjects]})
		df.to_csv(OUTPUT_FOLDER + 'fold_' + str(i) + '_predictions.csv', sep=',')
		accuracies.append(acc)
		# Best fold check.
		if acc > best_accuracy:
			best_fold = i
			best_accuracy = acc
			best_fold_predictions = pred
			best_fold_test_subjects = node_flags[test_subjects]
	
	# Extract execution statistics to a file.
	statistics = 'K-Fold validation statistics:' + '\nBest fold: ' + str(best_fold) + '\nBest accuracy: ' + str(best_accuracy) + '\nMean accuracy: ' + str(np.mean(accuracies)) + '\nStandard deviation: ' + str(np.std(accuracies))
	logging.info(statistics)
	with open(OUTPUT_FOLDER + 'classification_statistics.txt', "w") as output_file:
		output_file.write(statistics)
	# Extract best fold predictions to a file.
	df = pd.DataFrame({"Predicted": best_fold_predictions, "True": best_fold_test_subjects})
	df.to_csv(OUTPUT_FOLDER + 'best_fold_predictions.csv', sep=',')

#####################################################

# Script execution order:
#	1. Create execution output folder.
#	2. Retrieve execution records dictionary.
#	3. Generate StellarGraph object.
#	4. Execute Machine Learning task.

total_time = time.time()
OUTPUT_FOLDER = create_output_folder()
execution_records_dict = retrieve_execution_records()
stellar_graph, node_flags = generate_graph(execution_records_dict)
execute_graph_ML(stellar_graph, node_flags)
logging.info('Total Execution time: ' + time.strftime('%H:%M:%S', time.gmtime(time.time() - total_time)))
