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
import entities as entity
from entities import Record
from entities import Order
from entities import FullOrder
from entities import User

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
	return STATUS_SKIPPED


def update_e_record(e_record, point, db=None, table=None):
	"""
	Check if it is a fresh order and write it to db
	:param record: dictionary with fields after ETL
	:return: STATUS_INSERTED | STATUS_REPLACED | STATUS_SKIPPED
	"""
	try:
		# record = ETL.replace_nan(record, record.keys(), None)  # ??
		old_record = db[table].find_one({'_id': e_record.__dict__.get('_id', None)})
		if old_record is None:
			db[table].insert_one(e_record.to_row(prefix=""))
			# this is absolutely new order
			return STATUS_INSERTED

		if old_record['updated_at'] is None:
			old_record['updated_at'] = VERY_EARLY_DATE  # protect from None instead of date

		if old_record.get('updated_at', VERY_EARLY_DATE) < e_record.__dict__.get('updated_at', VERY_EARLY_DATE):
			# overwrite if current record is later
			db[table].save(e_record.to_row())
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
	e_order = Order(order)
	have_errors, error = e_order.have_errors()
	if not have_errors:
		# return update_e_record(e_order, point, db=db, table=table)
		return e_order.update(point, db=db, table=table)
	else:
		# order = ETL.prepare_order(order)
		# order['error_info'] = error
		e_order.set('error_info', error)
		error_coll = point.get('error_orders', "error_orders")
		db[error_coll].save(e_order.to_row(prefix=""))
		return None


def update_efull_order(e_full_order, point, db=None, table=None):
	old_record = db[table].find_one({'_id': e_full_order.__dict__.get('_id', None)})
	if old_record is None:
		db[table].insert_one(e_full_order.to_row())
		# this is absolutely new order
		return STATUS_INSERTED

	if old_record['updated_at'] is None:
		old_record['updated_at'] = VERY_EARLY_DATE  # protect from None instead of date

	if old_record.get('updated_at', VERY_EARLY_DATE) < e_full_order.updated_at:
		# overwrite if current record is later
		db[table].save(e_full_order.to_row())
		return STATUS_REPLACED

	return STATUS_SKIPPED


def update_full_order(full_order, point, db=None, table=None):
	"""
	Check if it is a fresh order and write it to db
	:param order: dictionary with fields
	:return: STATUS_INSERTED | STATUS_REPLACED | STATUS_SKIPPED
	"""
	full_order = ETL.prepare_full_order(full_order)
	e_full_order = FullOrder.parse(full_order)
	have_errors, error = e_full_order.have_errors()
	if not have_errors:
		# old_record = db[table].find_one({'_id': e_full_order.__dict__.get('_id', None)})
		return update_efull_order(e_full_order, point, db=db, table=table)
	# return update_record(full_order, point, db=db, table=table)
	else:
		# full_order = ETL.prepare_full_order(full_order)
		# full_order['error_info'] = error
		e_full_order.set('error_info', error)
		updated_at = str(e_full_order.get('updated_at', ""))
		_id = e_full_order.get('id', "") + "==" + str(updated_at)
		e_full_order.set('_id', _id)
		error_coll = point.get('error_full_orders', "error_full_orders")
		db[error_coll].save(e_full_order.to_row())
		return None


def update_user(user, point, db=None, table=None):
	# user = ETL.prepare_user(user)
	e_user = User(user)
	have_errors, error = e_user.have_errors()
	# is_correct, error = ETL.is_user_correct(user)
	if not have_errors:
		# user = ETL.prepare_user(user)
		return update_e_record(e_user, point, db=db, table=table)
	# return update_record(user, point, db=db, table=table)
	else:
		e_user.set('error_info', error)
		error_coll = point.get('error_users', "error_users")
		db[error_coll].save(user)
		return None
