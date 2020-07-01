# -*- coding: utf-8 -*-
# -*- coding: utf-8 -*-
__author__ = "Veaceslav Kunitki"
__copyright__ = "Copyright 2020. Please inform me in case of usage"
__credits__ = ["No credits"]
__license__ = "MIT"
__version__ = "11.111111111"
__maintainer__ = "Veaceslav Kuntiki"
__email__ = "tumikosha@gmail.com"
__status__ = "Prototype"
"""
Utils for working with mongoDB
"""
import pymongo
import config
import numpy as np
import datetime, dateparser
import ETL
import sys

db = None
client = None

# default date
VERY_EARLY_DATE = dateparser.parse("1 January 1970")


def create_zlib_collection(xdb, name):
	"""makes mongodb collection in ZIPPED MODE"""
	print('[X]creating collection: ', xdb, name)
	try:
		xdb.create_collection(name,
							  storageEngine={'wiredTiger': {'configString': 'block_compressor=zlib'}})
		print('ZLIB collection created')
	except:
		print('Collection already exists')


def ensure_index(xdb, collection, field, direction):
	"""create index if it is not exists"""
	print('[X]creating index:', collection, field, direction)
	try:
		response = xdb[collection].create_index([(field, direction)])
		print(response)
	except Exception as e:
		print(str(e))


def prepare_database():
	""" singletone
	makes database connection
	:return: (client, database)
	"""
	global db, client
	if client is not None:
		return client, db

	client = pymongo.MongoClient(config.MONGO_URI)
	db = client[config.DB_NAME]
	create_zlib_collection(db, "orders")
	create_zlib_collection(db, "users")
	ensure_index(db, 'orders', 'created_at', pymongo.ASCENDING)
	ensure_index(db, 'orders', 'created_at', pymongo.DESCENDING)
	ensure_index(db, 'customers', 'created_at', pymongo.ASCENDING)
	ensure_index(db, 'customers', 'updated_at', pymongo.ASCENDING)
	return client, db


def correct_encoding(dictionary):
	"""Correct the encoding of python dictionaries so they can be encoded to mongodb
	inputs
	-------
	dictionary : dictionary instance to add as document
	output
	-------
	new : new dictionary with (hopefully) corrected encodings"""

	new = {}
	for key1, val1 in dictionary.items():
		# Nested dictionaries
		if isinstance(val1, dict):
			val1 = correct_encoding(val1)

		if isinstance(val1, np.bool_):
			val1 = bool(val1)

		if isinstance(val1, np.int64):
			val1 = int(val1)

		if isinstance(val1, np.float64):
			val1 = float(val1)

		new[key1] = val1

	return new


STATUS_INSERTED = 0
STATUS_REPLACED = 1
STATUS_SKIPPED = 2



def clear_database():
	db.orders.delete_many({})  # delete all orers
	db.customers.delete_many({})  # delete all customers/users
	print("ATTENTION! -----======DATABASE ERASED====-------")


def write_df_to_mongoDB(my_df, \
						database_name='mydatabasename', \
						collection_name='mycollectionname',
						server='localhost', \
						mongodb_port=27017, \
						chunk_size=100):
	# """
	# This function take a list and create a collection in MongoDB (you should
	# provide the database name, collection, port to connect to the remoete database,
	# server of the remote database, local port to tunnel to the other machine)
	#
	# ---------------------------------------------------------------------------
	# Parameters / Input
	#    my_list: the list to send to MongoDB
	#    database_name:  database name
	#
	#    collection_name: collection name (to create)
	#    server: the server of where the MongoDB database is hosted
	#        Example: server = '132.434.63.86'
	#    this_machine_port: local machine port.
	#        For example: this_machine_port = '27017'
	#    remote_port: the port where the database is operating
	#        For example: remote_port = '27017'
	#    chunk_size: The number of items of the list that will be send at the
	#        some time to the database. Default is 100.
	#
	# Output
	#    When finished will print "Done"
	# ----------------------------------------------------------------------------
	# FUTURE modifications.
	# 1. Write to SQL
	# 2. Write to csv
	# ----------------------------------------------------------------------------
	# 30/11/2017: Rafael Valero-Fernandez. Documentation
	# """

	# To connect
	# import os
	# import pandas as pd
	# import pymongo
	# from pymongo import MongoClient

	client = pymongo.MongoClient('localhost', int(mongodb_port))
	db = client[database_name]
	collection = db[collection_name]
	# To write
	collection.delete_many({})  # Destroy the collection
	# aux_df=aux_df.drop_duplicates(subset=None, keep='last') # To avoid repetitions
	my_list = my_df.to_dict('records')
	l = len(my_list)
	ran = range(l)
	steps = ran[chunk_size::chunk_size]
	steps.extend([l])

	# Inser chunks of the dataframe
	i = 0
	for j in steps:
		print(j)
		collection.insert_many(my_list[i:j])  # fill de collection
		i = j

	print('Done')
	return


def clear_point(point):
	if point['erase_point_on_start']:
		print("ERASING point ", point)
		if point['type'].find('mongo') > -1:
			client = pymongo.MongoClient(point['uri'])
			db = client[point['db_name']]
			db['users'].remove({})
			db['orders'].remove({})
			db['full_orders'].remove({})


def update_record(record, point, db=None, table=None):
	"""
	Check if it is a fresh order and write it to db
	:param record: dictionary with fields after ETL
	:return: STATUS_INSERTED | STATUS_REPLACED | STATUS_SKIPPED
	"""
	try:
		record = ETL.replace_nan(record, record.keys(), None)  # ??
		old_record = db[table].find_one({'_id': record['_id']})
		if old_record is None:
			db[table].insert_one(record)
			# this is absolutely new order
			return STATUS_INSERTED

		if old_record['updated_at'] is None:
			old_record['updated_at'] = VERY_EARLY_DATE  # protect from None instead of date

		if old_record.get('updated_at', VERY_EARLY_DATE) < record['updated_at']:
			# overwrite if current record is later
			db[table].save(record)
			return STATUS_REPLACED
	# if we are here, the order is old and no need to update
	except Exception as e:
		print(str(e))

		sys.exit()
	return STATUS_SKIPPED


def point_connection(point):
	"""return connection to dabase """
	client = pymongo.MongoClient(point['uri'])
	db = client[point['db_name']]
	return client, db


def update_order(order, point, db=None, table=None):
	"""
	Check if it is a fresh order and write it to db
	:param order: dictionary with fields
	:return: STATUS_INSERTED | STATUS_REPLACED | STATUS_SKIPPED
	"""
	is_correct, error = ETL.is_order_correct(order)
	if is_correct:
		order = ETL.prepare_order(order)
		return update_record(order, point, db=db, table=table)
	else:
		order = ETL.prepare_order(order)
		order['error_info']= error
		error_coll = point.get('error_orders', "error_orders")
		db[error_coll].save(order)
		return None


def update_full_order(full_order, point, db=None, table=None):
	"""
	Check if it is a fresh order and write it to db
	:param order: dictionary with fields
	:return: STATUS_INSERTED | STATUS_REPLACED | STATUS_SKIPPED
	"""
	is_correct, error = ETL.is_full_order_correct(full_order)
	if is_correct:
		full_order = ETL.prepare_full_order(full_order)
		return update_record(full_order, point, db=db, table=table)
	else:
		full_order = ETL.prepare_full_order(full_order)
		full_order['error_info'] = error
		error_coll = point.get('error_full_orders', "error_full_orders")
		db[error_coll].save(full_order)
		return None


def update_user(user, point, db=None, table=None):
	# user = ETL.prepare_user(user)
	is_correct, error = ETL.is_user_correct(user)
	if is_correct:
		user = ETL.prepare_user(user)
		return update_record(user, point, db=db, table=table)
	else:
		user = ETL.prepare_user(user)
		user['error_info'] = error
		error_coll = point.get('error_users', "error_users")
		db[error_coll].save(user)
		return None

