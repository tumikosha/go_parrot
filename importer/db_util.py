# -*- coding: utf-8 -*-
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


def mention_user(user_id):
	"""if user found only in order file 'is_empty': True"""
	try:
		# insert user mentioned in order but without first_name and other fields
		# db.customers.insert_one({'_id': user_id, 'user_id': user_id, 'updated_at': VERY_EARLY_DATE, 'is_empty': True})
		db.customers.insert_one({'_id': user_id, 'user_id': user_id, 'updated_at': None, 'is_empty': True})
	# db.customers.insert_one({'_id': user_id, 'user_id': user_id, 'is_empty': True})

	except Exception as e:
		pass  # it is ok. this user already exists


# def update_order(order):
# 	"""
# 	Check if it is a fresh order and write it to db
# 	:param order: dictionary with fields
# 	:return: STATUS_INSERTED | STATUS_REPLACED | STATUS_SKIPPED
# 	"""
# 	order['_id'] = order['id']
# 	old_order = db.orders.find_one({'_id': order['id']})
# 	mention_user(order['user_id'])
# 	if old_order is None:
# 		db.orders.insert_one(order)
# 		return STATUS_INSERTED
# 	# this is absolutely new order
# 	if old_order['updated_at'] is None:  old_order['updated_at'] = VERY_EARLY_DATE  # protect from None instead of date
# 	if old_order.get('updated_at', VERY_EARLY_DATE) < order['updated_at']:
# 		db.orders.save(order)
# 		return STATUS_REPLACED
# 	# if we are here, the order is old and no need to update
# 	return STATUS_SKIPPED


# def update_user(user):
# 	"""
# 	check if it is fresh order and write it to db
# 	:param user: dictionary with fields
# 	:return: STATUS_INSERTED | STATUS_REPLACED | STATUS_SKIPPED
# 	"""
# 	global db
#
# 	# db.customers.insert_one(order)
# 	user['_id'] = user['user_id']
# 	user['is_empty'] = False
# 	old_user = db.customers.find_one({'_id': user['user_id']})
# 	if old_user is None:
# 		db.customers.insert_one(user)
# 		return STATUS_INSERTED
# 	# this is absolutely new order
# 	# print(old_user['updated_at'], user['updated_at'])
# 	if old_user['updated_at'] is None: old_user['updated_at'] = VERY_EARLY_DATE  # protect from None instead of date
# 	if old_user.get('updated_at', VERY_EARLY_DATE) < user['updated_at']:
# 		db.customers.save(user)
# 		return STATUS_REPLACED
# 	# if we are here, the user is old and no need to update
# 	return STATUS_SKIPPED


# user['_id'] = user['user_id']
# db.customers.save(user)


def order_range_in_db(db) -> (datetime.datetime, datetime.datetime):
	""" maximal and minimal date of order in db
		return: (min_date, max_date)
	"""
	if db.orders.count_documents({}) == 0:
		# if db.orders.count({}) == 0:
		return dateparser.parse("January 1th, 1973 00:00"), dateparser.parse("January 1th, 1973 00:00")
	first_order = db.orders.find_one({}, sort=[('created_at', pymongo.ASCENDING)])
	last_order = db.orders.find_one({}, sort=[('created_at', pymongo.DESCENDING)])
	return first_order['created_at'], last_order['created_at']


def clear_database():
	db.orders.delete_many({})  # delete all orers
	db.customers.delete_many({})  # delete all customers/users
	print("ATTENTION! -----======DATABASE ERASED====-------")


def digest():
	total_orders = db.orders.count_documents({})
	total_customers = db.customers.count_documents({})
	total_empty = db.customers.count_documents({'is_empty': True})
	return total_orders, total_customers, total_empty


def order_range_in_db(db) -> (datetime.datetime, datetime.datetime):
	""" maximal and minimal date of order in db
		return: (min_date, max_date)
	"""
	if db.orders.count_documents({}) == 0:
		return VERY_EARLY_DATE, VERY_EARLY_DATE
	first_order = db.orders.find_one({}, sort=[('updated_at', pymongo.ASCENDING)])
	last_order = db.orders.find_one({}, sort=[('updated_at', pymongo.DESCENDING)])
	return first_order['updated_at'], last_order['updated_at']


def customer_range_in_db(db) -> (datetime.datetime, datetime.datetime):
	""" maximal and minimal date of customer in db
		return: (min_date, max_date)
	"""
	if db.customers.count_documents({'is_empty': False}) == 0:
		return VERY_EARLY_DATE, VERY_EARLY_DATE
	first_customer = db.customers.find_one({'is_empty': False}, sort=[('updated_at', pymongo.ASCENDING)])
	last_customer = db.customers.find_one({'is_empty': False}, sort=[('updated_at', pymongo.DESCENDING)])
	return first_customer['updated_at'], last_customer['updated_at']


def info():
	"""show database digest"""
	print("----------------mongodb digest----------------")
	total_orders = db.orders.count_documents({})
	total_customers = db.customers.count_documents({})
	# total_empty = db.customers.count_documents({'first_name': {"$exists": False}})
	total_empty = db.customers.count_documents({'is_empty': True})
	print("Total records in DB.orders:", total_orders)
	print("Total users in db.customers:", total_customers)
	print("Empty users in db.customers:", total_empty)
	first_dt, last_dt = order_range_in_db(db)
	if first_dt == VERY_EARLY_DATE: first_dt = None
	if last_dt == VERY_EARLY_DATE: last_dt = None

	print("order range:", first_dt, " --->", last_dt)
	customer_first_dt, customer_last_dt = customer_range_in_db(db)
	if customer_first_dt == VERY_EARLY_DATE: customer_first_dt = None
	if customer_last_dt == VERY_EARLY_DATE: customer_last_dt = None
	print("customer range:", customer_first_dt, " --->", customer_last_dt)
	print("-------------------------------")
	return total_orders, total_customers, total_empty


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

# def update_user(user, db=None, table=None):
# 	"""
# 	check if it is fresh order and write it to db
# 	:param user: dictionary with fields
# 	:return: STATUS_INSERTED | STATUS_REPLACED | STATUS_SKIPPED
# 	"""
# 	user['_id'] = user['user_id']
# 	user['is_empty'] = False
# 	if user.get('user_updated_at', None) is not None:
# 		user['updated_at'] = user.get('user_updated_at', None)
# 		del user['user_updated_at']
#
# 	old_user = db[table].find_one({'_id': user['user_id']})
# 	if old_user is None:
# 		db[table].insert_one(user)
# 		return STATUS_INSERTED
# 	# this is absolutely new order
# 	# print(old_user['updated_at'], user['updated_at'])
# 	if old_user['updated_at'] is None: old_user['updated_at'] = VERY_EARLY_DATE  # protect from None instead of date
# 	if old_user.get('updated_at', VERY_EARLY_DATE) < user['updated_at']:
# 		db[table].save(user)
# 		return STATUS_REPLACED
# 	# if we are here, the user is old and no need to update
# 	return STATUS_SKIPPED
