# -*- coding: utf-8 -*-
import pymongo
import config
import numpy as np
import datetime, dateparser

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
	db = client['go_parrot']
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
		db.customers.insert_one({'_id': user_id, 'user_id': user_id, 'updated_at': VERY_EARLY_DATE, 'is_empty': True})
	except Exception as e:
		pass  # it is ok. this user already exists


def update_order(order):
	"""
	Check if it is a fresh order and write it to db
	:param order: dictionary with fields
	:return: STATUS_INSERTED | STATUS_REPLACED | STATUS_SKIPPED
	"""
	order['_id'] = order['id']
	old_order = db.orders.find_one({'_id': order['id']})
	mention_user(order['user_id'])
	if old_order is None:
		db.orders.insert_one(order)
		return STATUS_INSERTED
	# this is absolutely new order
	if old_order['updated_at'] < order['updated_at']:
		db.orders.save(order)
		return STATUS_REPLACED
	# if we are here, the order is old and no need to update
	return STATUS_SKIPPED


def update_user(user):
	"""
	check if it is fresh order and write it to db
	:param user: dictionary with fields
	:return: STATUS_INSERTED | STATUS_REPLACED | STATUS_SKIPPED
	"""
	global db

	# db.customers.insert_one(order)
	user['_id'] = user['user_id']
	user['is_empty'] = False
	old_user = db.customers.find_one({'_id': user['user_id']})
	if old_user is None:
		db.customers.insert_one(user)
		return STATUS_INSERTED
	# this is absolutely new order
	# print(old_user['updated_at'], user['updated_at'])
	if old_user['updated_at'] < user['updated_at']:
		db.customers.save(user)
		return STATUS_REPLACED
	# if we are here, the user is old and no need to update
	return STATUS_SKIPPED


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
	if db.orders.count_documents({}) == 0:
		return VERY_EARLY_DATE, VERY_EARLY_DATE
	first_customer = db.customers.find_one({}, sort=[('updated_at', pymongo.ASCENDING)])
	last_customer = db.customers.find_one({}, sort=[('updated_at', pymongo.DESCENDING)])
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
	print("order range:", first_dt, " --->", last_dt)
	customer_first_dt, customer_last_dt = customer_range_in_db(db)
	print("customer range:", customer_first_dt, " --->", customer_last_dt)
	print("-------------------------------")
	return total_orders, total_customers, total_empty
