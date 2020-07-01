# import petl as etl, psycopg2 as pg, sys
import petl, os
import yaml
import sys
import pandas as pd
from pprint import pprint
import datetime, dateparser
import pymongo
import db_util, ETL
import tqdm
import argparse
import logging
from flask import Flask
from flask_crontab import Crontab
from enum import Enum

# configure logger
logger = logging.getLogger('import_app')
logger.setLevel(logging.INFO)
# create file handler which logs even debug messages
fh = logging.FileHandler('_importer.log')
fh.setLevel(logging.DEBUG)
# create console handler with a higher log level
ch = logging.StreamHandler()
ch.setLevel(logging.ERROR)
# create formatter and add it to the handlers
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fh.setFormatter(formatter)
ch.setFormatter(formatter)
# add the handlers to the logger
logger.addHandler(fh)
logger.addHandler(ch)
logger.info('[x] Flask importer app runned')

app = Flask(__name__)
crontab = Crontab(app)  # Flask crontab


def csv_2_df(path, dtype=None) -> pd.DataFrame:
	"""
	load CSV to pandas dataframe
	:param path: path to CSV file
	:param dtype: list with DTYPES see read_csv description for details
	:return: pd.DataFrame
	"""
	f = lambda s: datetime.datetime.strptime(s, '%Y-%m-%d %H:%M:%S')
	df_ = pd.read_csv(path, date_parser=f, dtype=dtype, parse_dates=['created_at', 'updated_at'])
	df_['updated_at'] = pd.to_datetime(df_['updated_at'])
	df_['created_at'] = pd.to_datetime(df_['created_at'])
	return df_


# def load_all_files(dir, start_with):
# 	order_files = [f for f in os.listdir(dir) if f.startswith(start_with)]
# 	count = 0
# 	for file in order_files:
# 		orders_ = petl.fromcsv(dir + "/" + file)
# 		if count == 0:
# 			all = orders_
# 		else:
# 			all = petl.merge(all, orders_, key="updated_at")
# 		count += 1
# 	return all

def point_iterator(yaml_path, tip='csv_orders'):
	with open(yaml_path) as f:
		config = yaml.safe_load(f)
	for source_name_, source_ in config['points'].items():
		if source_['type'] == tip:
			yield source_name_, source_, source_['type']


def df_iterator(yaml_path, query, tip='csv_orders', start=db_util.VERY_EARLY_DATE, end=datetime.datetime.now()):
	with open(yaml_path) as f:
		config = yaml.safe_load(f)
	for source_name_, source_ in config['points'].items():
		if source_['type'] == tip:
			f = lambda s: datetime.datetime.strptime(s, '%Y-%m-%d %H:%M:%S')
			if tip == 'csv_orders' or tip == 'csv_users':
				df = pd.read_csv(source_['uri'], dtype=source_['dtype'], index_col=False,
								 date_parser=f, parse_dates=['created_at', 'updated_at'])

				yield source_name_, source_, df
			# if tip == 'mongo_orders_source':
			if tip.find("mongo_source") > -1:
				arr = []
				client, db = db_util.point_connection(source_)
				q = {"$and": [query, {'updated_at': {'$gte': start}}, {'updated_at': {'$lte': end}}]}
				print('start:', str(start), ":::>", str(end))
				print(q)
				cursor = db[source_['table_name']].find(q)
				# Expand the cursor and construct the DataFrame
				df = pd.DataFrame(list(cursor))
				cursor.close()
				# print("DF::", df)
				if len(df) != 0:
					df = df.drop(['_id'], axis=1)
				else:
					df = None
				yield source_name_, source_, df


class IterType(Enum):
	POINT = "point"
	DF = 'df'
	ROW = 'row'


class PointType(Enum):
	CSV_ORDERS = "csv_orders"
	CSV_USERS = "csv_users"
	ORDERS_MONGO_SOURCE = 'orders_mongo_source'
	USERS_MONGO_SOURCE = 'users_mongo_source'
	MONGO_DEST = 'mongo_dest'
	CSV_DEST = 'CSV_dest'


def row_iterator(yaml_path, query, tip=PointType.CSV_ORDERS, start=db_util.VERY_EARLY_DATE,
				 end=datetime.datetime.now()):
	with open(yaml_path) as f:
		config = yaml.safe_load(f)
	for source_name_, source_ in config['points'].items():
		if source_['type'] == tip:
			if tip == PointType.CSV_ORDERS:
				f = lambda s: datetime.datetime.strptime(s, '%Y-%m-%d %H:%M:%S')
				df = pd.read_csv(source_['uri'], dtype=source_['dtype'], index_col=False,
								 date_parser=f, parse_dates=['created_at', 'updated_at'])
				for idx, row_ in df.T.iteritems():
					yield source_name_, source_, row_

			# if tip == 'mongo_orders_source':
			# if tip.find("mongo_source") > -1:
			if tip in [PointType.ORDERS_MONGO_SOURCE, PointType.USERS_MONGO_SOURCE]:
				arr = []
				client, db = db_util.point_connection(source_)
				q = {"$and": [query, {'updated_at': {'$gte': start}}, {'updated_at': {'$lte': end}}]}
				print('start:', str(start), ":::>", str(end))
				print(q)
				cursor = db[source_['table_name']].find(q, no_cursor_timeout=True)
				for row in cursor:
					yield source_name_, source_, row
				cursor.close()


def universal_iterator(yaml_path, query, tip=PointType.CSV_ORDERS,
					   return_type=IterType.POINT,
					   start=dateparser.parse("100 years ago"),
					   end=dateparser.parse("2 days in")):

	if return_type == IterType.POINT:
		for a, b, c in point_iterator(yaml_path, tip=tip):
			yield a, b, c

	if return_type == IterType.DF:
		for a, b, c in df_iterator(yaml_path, {}, tip=tip, start=start, end=end):
			yield a, b, c

	if return_type == IterType.ROW:
		for a, b, c in row_iterator(yaml_path, {}, tip=tip, start=start, end=end):
			yield a, b, c


# def universal_iterator(yaml_path, query, tip='csv_orders', return_type='row', start=db_util.VERY_EARLY_DATE,
# 					   end=datetime.datetime.now()):
# 	"""
# 	Iterates over all points with specified type
# 	The idea is to have one universal iterator over all sources
# 	:param yaml_path: path to YAML file
# 	:param query:  TODO
# 	:param tip:  point type to iterate [csv_orders, csv_users, orders_mongo_source, users_mongo_source, csv_dest, mongo_dest]
# 	:param return_type:  ['point' | 'df' | 'row']
# 	:param start:  start time  (WARNING do not supported for `point` and `row` iteration) TODO
# 	:param end:  end time (WARNING do not supported for `point` and `row` iteration) TODO
# 	:return: generator  (source_name_, source_, ITEM) where ITEM can be Point dict |  DataFrame | ROW dict
# 	:rtype:  str, dict, obj
# 	"""
#
# 	with open(yaml_path) as f:
# 		config = yaml.safe_load(f)
#
# 	for source_name_, source_ in config['points'].items():
# 		if source_['type'] == tip:
# 			if return_type == 'point':
# 				yield source_name_, source_, source_['type']
# 			# return
#
# 			if return_type == 'df':
# 				f = lambda s: datetime.datetime.strptime(s, '%Y-%m-%d %H:%M:%S')
# 				if tip == 'csv_orders' or tip == 'csv_users':
# 					df = pd.read_csv(source_['uri'], dtype=source_['dtype'], index_col=False,
# 									 date_parser=f, parse_dates=['created_at', 'updated_at'])
#
# 					yield source_name_, source_, df
# 				# if tip == 'mongo_orders_source':
# 				if tip.find("mongo_source") > -1:
# 					arr = []
# 					client, db = db_util.point_connection(source_)
# 					q = {"$and": [query, {'updated_at': {'$gte': start}}, {'updated_at': {'$lte': end}}]}
# 					print('start:', str(start), ":::>", str(end))
# 					print(q)
# 					cursor = db[source_['table_name']].find(q)
# 					# Expand the cursor and construct the DataFrame
# 					df = pd.DataFrame(list(cursor))
# 					cursor.close()
# 					# print("DF::", df)
# 					if len(df) != 0:
# 						df = df.drop(['_id'], axis=1)
# 					else:
# 						df = None
# 					yield source_name_, source_, df
#
# 			if return_type == 'row':
# 				if tip == 'csv_orders':
# 					f = lambda s: datetime.datetime.strptime(s, '%Y-%m-%d %H:%M:%S')
# 					df = pd.read_csv(source_['uri'], dtype=source_['dtype'], index_col=False,
# 									 date_parser=f, parse_dates=['created_at', 'updated_at'])
# 					for idx, row_ in df.T.iteritems():
# 						yield source_name_, source_, row_
# 				# if tip == 'mongo_orders_source':
# 				if tip.find("mongo_source") > -1:
# 					arr = []
# 					client, db = db_util.point_connection(source_)
# 					cursor = db[source_['table_name']].find(query, no_cursor_timeout=True)
# 					# Expand the cursor and construct the DataFrame
# 					for row in cursor:
# 						# del row['_id']
# 						yield source_name_, source_, row


def load_and_filter_all_users(cfg_path_, tip="csv_users", start=db_util.VERY_EARLY_DATE,
							  end=datetime.datetime.now()) -> (pd.DataFrame, pd.DataFrame):
	"""
	Load YAML, Iterates over all points with specified type==tip as DataFrames
	merge all iterated dataframes
	converts datafields to `datetime` type
	and makes GROUP BY
	:param str cfg_path_: path to YAML file
	:param str tip:  point type to iterate [csv_users ONLY!]
	:param datetime.datetime  start:  start time  (WARNING do not supported for `point` and `row` iteration) TODO
	:param datetime.datetime  end:  end time (WARNING do not supported for `point` and `row` iteration) TODO
	:return: df_summ, df_summ_filtered - not_filterd and filtered dataframes with users from all source points
	:rtype:  (pd.DataFrame, pd.DataFrame)
	"""

	df_summ = None
	for source_name, source, df in universal_iterator(cfg_path_, {}, tip=tip, return_type=IterType.DF, start=start, end=end):
		if df_summ is None:
			df_summ = df
		else:
			df_summ = pd.concat([df, df_summ], ignore_index=True)
	if df_summ is not None:
		df_summ['updated_at'] = pd.to_datetime(df_summ['updated_at'])
		df_summ['created_at'] = pd.to_datetime(df_summ['created_at'])
		df_summ_filtered = df_summ.loc[df_summ.groupby("user_id")["updated_at"].idxmax()]  # keep max updated_at
	else:
		df_summ_filtered = None
	return df_summ, df_summ_filtered


def load_and_filter_all_orders(cfg_path_, tip="csv_orders", start=db_util.VERY_EARLY_DATE,
							   end=datetime.datetime.now()) -> (pd.DataFrame, pd.DataFrame):
	"""
	Load YAML, Iterates over all points with specified type==tip as DataFrames
	merge all iterated dataframes
	converts datafields to `datetime` type
	and makes GROUP BY
	:param str cfg_path_: path to YAML file
	:param str tip:  point type to iterate [csv_orders ONLY]
	:param datetime.datetime start:  start time  (WARNING do not supported for `point` and `row` iteration) TODO
	:param datetime.datetime end:  end time (WARNING do not supported for `point` and `row` iteration) TODO
	:return:  not_filterd and filtered dataframes with users from all source points
	:rtype:  (pd.DataFrame, pd.DataFrame)
	"""

	df_summ = None
	for source_name, source, df in universal_iterator(cfg_path_, {}, tip=tip, return_type=IterType.DF, start=start, end=end):
		if df_summ is None:
			df_summ = df
		else:
			df_summ = pd.concat([df, df_summ], ignore_index=True)
	if df_summ is not None:
		df_summ['updated_at'] = pd.to_datetime(df_summ['updated_at'])
		df_summ['created_at'] = pd.to_datetime(df_summ['created_at'])
		df_summ['id'] = df_summ['id'].astype(str)
		df_summ_filtered = df_summ.loc[df_summ.groupby("id")["updated_at"].idxmax()]  # keep max updated_at
	else:
		df_summ_filtered = None
	return df_summ, df_summ_filtered


def NaT(df, column) -> pd.DataFrame:
	"""
	Drop records with NaT values in column
	:param df: path to YAML file
	:param column:  point type to iterate [csv_orders ONLY]
	:return: dataFrame with changed cells
	:rtype: pd.DataFrame
	"""
	df[[column]] = df[[column]].astype(object).where(
		df[[column]].notnull(), None)
	return df


def df_to_mongo(df, uri, db_name, collection_name, point, desc="write to mongo",
				etl=lambda record: record,  # default: no etl
				func=db_util.update_record) -> None:
	"""
	write dataframe to mongodb collection
	:param df: pandas.DataFrame
	:param uri:  mongpdb uri
	:param db_name:
	:param collection_name:
	:param point: dict, sometimes need parameters for this point from YAMl file
	:param desc:
	:param etl: ROW transformation function
	:param func: function for update record in db
	:return:  None
	"""

	client = pymongo.MongoClient(uri)
	db = client[db_name]
	with tqdm.tqdm(total=len(df), desc=desc) as pbar:
		for idx, row_ in df.T.iteritems():
			# record = dict(row_)
			record = row_.to_dict()
			record = etl(record)
			# db_util.update_record(record, db=db, table=collection_name)
			func(record, point, db=db, table=collection_name)
			pbar.update(1)


def merge_df(df1, df2) -> pd.DataFrame:
	"""merge 2 dataframes with None control
	:param df1: pd.dataFrame
	:param df2: pd.dataFrame
	:return:  merged dataframe
	:rtype: result pd.DataFrame or None
	"""
	if (df1 is None) and (df2 is None):
		return None
	if df1 is None: return df2
	if df2 is None: return df1
	df_ = pd.concat([df1, df2], ignore_index=True)
	return df_


def fix_df_users(df_users) -> pd.DataFrame:
	"""
	Fix wrong columns formats
	:param df_users: pd.dataFrame
	:return:  fixed dataframe
	:rtype: result pd.DataFrame
	"""

	if df_users is not None:
		df_users['user_created_at'] = df_users['created_at']
		df_users['user_updated_at'] = df_users['updated_at']
		df_users = df_users.drop(['created_at', 'updated_at'], axis=1)
	return df_users


# def process_yaml(cfg_path, start=db_util.VERY_EARLY_DATE, end=datetime.datetime.now()) -> int:
def process_yaml(cfg_path, start=db_util.VERY_EARLY_DATE, end=dateparser.parse("1 day in")) -> int:
	"""
	Life cycle function:
		Step1. Loads orders and users from different sources(aka points)
		Step2. Filter & grouping
		Step3. Join  Orders and Users
		Step4. write results to destination points

	:param str cfg_path: path to YAML file
	:param datetime.datetime start:  begin time
	:param datetime.datetime end: end time
	:return:  number of produced full_orders
	:rtype: int
	"""
	# STEP1. loading & Filtering
	print('processing', cfg_path)
	print('Loading CSV users...')
	df_users_unfiltered, df_users = load_and_filter_all_users(cfg_path, tip="csv_users")

	print('Loading CSV orders...')
	df_orders_unfiltered, df_orders = load_and_filter_all_orders(cfg_path, tip="csv_orders")

	print('Loading Mongo orders...')
	df_orders_unfiltered_M, df_orders_M = load_and_filter_all_orders(cfg_path, tip="orders_mongo_source", start=start,
																	 end=end)

	print('Loading Mongo users...')  # we ignore time range for users!
	df_users_unfiltered_M, df_users_M = load_and_filter_all_users(cfg_path, tip="users_mongo_source")
	df_users = fix_df_users(df_users)
	df_users_M = fix_df_users(df_users_M)
	df_users = merge_df(df_users, df_users_M)
	# print(df_users)
	# print(df_users_M)
	df_orders = merge_df(df_orders, df_orders_M)
	df_users_unfiltered = merge_df(df_users_unfiltered, df_users_unfiltered_M)
	df_orders_unfiltered = merge_df(df_orders_unfiltered, df_orders_unfiltered_M)

	# df_orders = ETL.NaT_2_None_all_cols(df_orders)
	# df_users = ETL.NaT_2_None_all_cols(df_users)
	# df_users_unfiltered = ETL.NaT_2_None_all_cols(df_users_unfiltered)
	# df_orders_unfiltered = ETL.NaT_2_None_all_cols(df_orders_unfiltered)

	# print(df_users)

	# Step 3.GROUP &  JOIN....
	print('Join orders and users...')
	if df_users is None:
		print("No users...")
	else:
		print("Users:", len(df_users))
	if df_orders is None:
		print("No orders...")
	else:
		print("Orders:", len(df_orders))

	if (df_users is not None) and (df_orders is not None):  # group and join
		# GROUP
		df_orders = df_orders.loc[df_orders.groupby("id")["updated_at"].idxmax()]  # keep max updated_at
		df_users = df_users.loc[df_users.groupby("user_id")["user_updated_at"].idxmax()]  # keep max updated_at
		# JOIN
		full_orders = df_orders.join(df_users.set_index('user_id'), on='user_id')
	else:
		full_orders = None

	# --- === Iterate over destination points and save buffers ===---

	if full_orders is None:
		print("No full_orders to proceed...")
		return 0

	# STEP 4. Saving results
	# --- === It can be Only One CSV destination ===---
	print('Saving  CSV destinations...')
	if full_orders is not None:
		full_orders.to_csv("data/full_orders.csv", na_rep='None')

	for source_name, source, point_type in \
			universal_iterator(cfg_path, {}, tip="mongo_dest", return_type=IterType.POINT):
		full_orders[['updated_at']] = full_orders[['updated_at']].astype(object).where(
			full_orders[['updated_at']].notnull(), None)
		# ERASING POINT TODO for all collections
		if source.get('erase_point_on_start', None) is not None:
			if source['erase_point_on_start'] == True:
				db_util.clear_point(source)

		# SAVE full_orders
		full_orders['_id'] = full_orders['id']
		df_to_mongo(full_orders, source['uri'], source['db_name'], source['table_name'], source,
					func=db_util.update_full_order,
					desc="write `full_orders` to mongo " + source_name)

		# save FILTERED ORDERS
		filtered_orders_table_name = source.get('filtered_orders_table_name', None)
		if filtered_orders_table_name is not None:
			df_orders['_id'] = df_orders['id']
			df_to_mongo(df_orders, source['uri'], source['db_name'], filtered_orders_table_name, source,
						func=db_util.update_order,
						desc="write `" + filtered_orders_table_name + "` to mongo " + source_name)
		# SAVE filtered users
		filtered_users_table_name = source.get('filtered_users_table_name', None)
		if filtered_users_table_name is not None:
			df_users['_id'] = df_users['user_id']
			df_users['created_at'] = df_users['user_created_at']
			df_users['updated_at'] = df_users['user_updated_at']  # TODO DROP
			df_users = df_users.drop(['user_updated_at'], axis=1)
			df_users = df_users.drop(['user_created_at'], axis=1)

			df_to_mongo(df_users, source['uri'], source['db_name'], filtered_users_table_name, source,
						func=db_util.update_user,
						desc="write `users` to mongo " + source_name)
		# SAVE unfltered orders
		unfiltered_orders_table_name = source.get('unfiltered_orders_table_name', None)
		if unfiltered_orders_table_name is not None:
			df_orders_unfiltered['_id'] = df_orders_unfiltered['id'] + "==" + df_orders_unfiltered[
				'updated_at'].astype(str)
			df_orders_unfiltered = NaT(df_orders_unfiltered, 'updated_at')
			df_orders_unfiltered = NaT(df_orders_unfiltered, 'created_at')
			df_to_mongo(df_orders_unfiltered, source['uri'], source['db_name'], unfiltered_orders_table_name, source,
						# func=db_util.update_record,
						func=db_util.update_record,
						desc="write `" + unfiltered_orders_table_name + "` to mongo " + source_name)
		# SAVE unfltered users
		unfiltered_users_table_name = source.get('unfiltered_users_table_name', None)
		if unfiltered_orders_table_name is not None:
			df_users_unfiltered['_id'] = df_users_unfiltered['user_id'] + "==" + df_users_unfiltered[
				'updated_at'].astype(str)
			df_users_unfiltered = NaT(df_users_unfiltered, 'updated_at')
			df_users_unfiltered = NaT(df_users_unfiltered, 'created_at')
			df_to_mongo(df_users_unfiltered, source['uri'], source['db_name'], unfiltered_users_table_name, source,
						func=db_util.update_record,
						desc="write `" + unfiltered_users_table_name + "` to mongo " + source_name)
	if full_orders is not None:
		return len(full_orders)
	else:
		return 0


# */5 * * * *   At every 5th minute
@crontab.job(minute="*/5")
def scheduled_job(  # start_moment=dateparser.parse('5 min ago'),
		end_moment=datetime.datetime.now()):
	"""
	scan dir `data/` for orders / users for specified period
	# :param start_moment:  moment  for start of period
	:param end_moment:  moment  for end of period
	:return: None
	"""
	print("\n --------------------------------------------------------")
	logger.info('flask cronjob started')
	try:
		now = datetime.datetime.now()
		moment = dateparser.parse("7 days ago 00:00")
		moment_next = dateparser.parse("2 days in 00:00")
		# scheduled_job(end_moment=moment)
		logger.info("Period: " + str(moment) + " ::: " + str(moment_next))
		num = process_yaml('cron.yaml', start=moment, end=moment_next)
		logger.info("orders imported: " + str(num))
	# logger.info("customers imported: " + str(total_customers2 - total_customers))
	# logger.info("empty added: " + str(total_empty2 - total_empty))

	except Exception as e:
		logger.error('scheduled_job: ' + str(e))

	logger.info('flask cronjob ended')


if __name__ == "__main__":
	parser = argparse.ArgumentParser(
		description='Copy orders and users from sources to dest described in `config.yaml`')
	parser.add_argument('-m', '--mode', type=str, choices=['all', 'simulate'], help="import all files or simulate",
						default='all')
	parser.add_argument('-f', '--freq', type=str, help="cron freq for simulating ex: 5min 12h 1M",
						default="1D")
	# default="12h")
	parser.add_argument('-s', '--start', type=str,
						help="start date for import period: `1 day ago`, `1 hours ago`, `1 year ago`...",
						default="4 months ago 00:00")
	parser.add_argument('-e', '--end', type=str,
						help="end of period: 1_day_ago`, `1_january_2020`, `1 year ago`...",
						default="1 day in")

	parser.add_argument('-y', '--yaml', type=str,
						help="YAML file with sources descriptions",
						default='cron.yaml')

	parser.add_argument('--erase', dest='erase', action='store_true', default=False)

	args = parser.parse_args()
	start = dateparser.parse(args.start.replace("_", " "))
	end = dateparser.parse(args.end.replace("_", " "))
	print("Period:", start, end)
	logger.info("Period: " + str(start) + " ::: " + str(end))
	# process_yaml(args.yaml, start=start, end=end)
	if args.mode == 'all':  # 'simulate'
		# process_yaml("step_1.yaml")
		# print("---===STEP 2===---")
		# process_yaml("step_2.yaml")
		process_yaml(args.yaml)
	# sys.exit()
	# process_yaml("config.yaml", start=start, end=end)
	# sim_range = pd.date_range(start=begin, end=now, freq=args.freq)
	# sim_range = pd.date_range(start=start, end=datetime.datetime.now(), freq="5min")

	if args.mode == 'simulate':
		# process_yaml("step_1_all.yaml")
		# print("---===STEP 2===---")
		# process_yaml("step_2.yaml")

		sim_range = pd.date_range(start=start, end=datetime.datetime.now(), freq="1D")
		total = 0
		with tqdm.tqdm(total=len(sim_range), desc="simulating time ") as pbar:
			for idx in range(len(sim_range) - 1):
				now = datetime.datetime.now()
				moment = sim_range[idx] - (now - dateparser.parse("7 days ago 00:00"))
				moment_next = sim_range[idx + 1]
				# scheduled_job(end_moment=moment)
				num = process_yaml(args.yaml, start=moment, end=moment_next)
				total += num
				print("Total:", total)
				pbar.update(1)
				pbar.set_description("---===  %s" % str(moment) + "===---")
				pbar.refresh()

		# start = dateparser.parse('2 years ago')
		# end = dateparser.parse('today')
		# process_yaml(args.yaml, start=start, end=end)
		sys.exit()

# flask crontab add
