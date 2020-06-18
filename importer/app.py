# -*- coding: utf-8 -*-
"""
this script imports all files with orders/users from directory `data/`
import all files:
	python app.py --mode all --dest mongodb://127.0.0.1:57017/admin
simulate 5min activity:
	python app.py --mode simulate -freq 5min  --dest mongodb://127.0.0.1:57017/admin
run service by cron:
	flask crontab add
"""
import logging, os
import datetime
import argparse
from flask import Flask
from flask_crontab import Crontab

import pandas as pd
import dateparser, tqdm, time, pymongo, sys
import config
import db_util

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

VERY_EARLY_DATE = db_util.VERY_EARLY_DATE  # dateparser.parse("January 1th, 1973 00:00")
# column types for csv files
USERS_DTYPES = {'user_id': 'S', 'phone_number': 'S', \
				'created_at': 'S', 'updated_at': 'S'}


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
		client, db = db_util.prepare_database()
		first_dt, last_dt = db_util.order_range_in_db(db)
		customer_first_dt, customer_last_dt = db_util.customer_range_in_db(db)
		total_orders, total_customers, total_empty = db_util.digest()
		process_all_order_files('data/', start_moment=last_dt, end_moment=end_moment)
		process_all_user_files('data/', start_moment=customer_last_dt, end_moment=end_moment)
		total_orders2, total_customers2, total_empty2 = db_util.digest()
		logger.info("orders imported: " + str(total_orders2 - total_orders))
		logger.info("customers imported: " + str(total_customers2 - total_customers))
		logger.info("empty added: " + str(total_empty2 - total_empty))

	except Exception as e:
		logger.error(str(e))

	logger.info('flask cronjob ended')


def csv_2_df(path, dtype=None):
	"""load csv to pandas dataframe """
	f = lambda s: datetime.datetime.strptime(s, '%Y-%m-%d %H:%M:%S')
	df_ = pd.read_csv(path, date_parser=f, dtype=dtype,
					  parse_dates=['created_at', 'updated_at'])
	return df_


def process_orders_file(path, file, start_moment=VERY_EARLY_DATE, end_moment=datetime.datetime.now()):
	"""
	load orders from file, filter it for period and write to mongodb table
	:param path: `data/` by default
	:param file:
	:param start_moment: for filtering
	:param end_moment: for filtering
	:return:
	"""
	df_orders = pd.read_csv(path + "/" + file, dtype={'user_id': 'S'})
	df_orders['updated_at'] = pd.to_datetime(df_orders['updated_at'])
	df = df_orders.copy(deep=True).sort_values(by=['updated_at'])
	df['updated_at'] = pd.to_datetime(df['updated_at'])
	# select only orders in period from start_date till now
	# mask = (df['created_at'] > start_moment) & (df['created_at'] <= end_moment)  # TODO <=
	mask = (df['updated_at'] > (start_moment - config.TIME_DELTA)) & (df['updated_at'] <= end_moment)  # TODO <=
	batch = df.loc[mask]
	desc = "..." + str(start_moment) + "-->" + str(end_moment) + "  " + file
	with tqdm.tqdm(total=len(batch), desc=desc) as pbar:
		# for index, row in df.iterrows():
		for index, row in batch.iterrows():
			try:
				order = row.to_dict()
				# user = resolve_user(dfu_, record['user_id'])
				db_util.update_order(order)
			except Exception as e:
				logging.error(str(e))  # print a message to the log
			pbar.update(1)


def process_all_order_files(dir, start_moment=VERY_EARLY_DATE, end_moment=datetime.datetime.now()):
	"""
	VERY_EARLY_DATE = `1900, 1 Jan` - default datetime
	process all files, filter:  `updated_at` > start_date
	:param dir:
	:param start_moment:
	:param end_moment:
	:return:
	"""

	order_files = [f for f in os.listdir(dir) if f.startswith('orders')]
	for file in order_files:
		process_orders_file(dir, file, start_moment=start_moment, end_moment=end_moment)


def process_user_file(path, file, start_moment=VERY_EARLY_DATE, end_moment=datetime.datetime.now()):
	"""
	load user info from file, filter it for period and write to mongodb table `customers`
	:param path: `data/` by default
	:param file:
	:param start_moment:
	:param end_moment:
	:return:
	"""
	# user_df = pd.read_csv(path + "/" + file, dtype=USERS_DTYPES)
	user_df = csv_2_df(path + "/" + file, dtype=USERS_DTYPES)
	user_df['updated_at'] = pd.to_datetime(user_df['updated_at'])
	user_df['updated_at'] = pd.to_datetime(user_df['updated_at'])
	mask = (user_df['updated_at'] >  (start_moment - config.TIME_DELTA)) & (user_df['updated_at'] <= end_moment)  # TODO <=
	batch = user_df.loc[mask]
	# TODO slicing
	# with tqdm.tqdm(total=len(user_df), desc=file) as pbar:
	desc = "..." + str(start_moment) + "-->" + str(end_moment) + "  " + file
	with tqdm.tqdm(total=len(batch), desc=desc) as pbar:
		# for index, row in user_df.iterrows():
		for index, row in batch.iterrows():
			try:
				user = row.to_dict()
				db_util.update_user(user)
			except Exception as e:
				logger.info(str(e))  # print a message to the log
			pbar.update(1)


def process_all_user_files(dir, start_moment=VERY_EARLY_DATE, end_moment=datetime.datetime.now()):
	"""
	 Loads all users from all files in dir, filter it for period and write to mongodb table `customers`
	:param dir:
	:param start_moment:
	:param end_moment:
	:return:
	"""
	order_files = [f for f in os.listdir(dir) if f.startswith('user')]
	for file in order_files:
		try:
			process_user_file(dir, file, start_moment=start_moment, end_moment=end_moment)
		except Exception as e:
			logger.error("File "+file+" :::>"+str(e))


if __name__ == '__main__':
	parser = argparse.ArgumentParser(description='Copy orders and users from files in data directory to mongo db')
	parser.add_argument('-d', "--dest", type=str, default=config.MONGO_URI,
						help="mongodb URI like mongodb://db_name:password@ip:port/admin")
	parser.add_argument('-p', "--path", type=str, default='data/',
						help="path to dir with files")
	parser.add_argument('-m', '--mode', type=str, choices=['all', 'simulate'], help="import all files or simulate",
						default='all')
	parser.add_argument('-f', '--freq', type=str, help="cron freq for simulating ex: 5min 12h 1M",
						default="12h")
	parser.add_argument('-t', '--timeshift', type=str, help="import only records for this period: `1 day ago`, `1 hours ago`, `1 year ago`...",
						default="100 years ago")


	args = parser.parse_args()
	config.MONGO_URI = args.dest

	client, db = db_util.prepare_database()
	# db.orders.delete_many({})  # just for tests
	if args.mode == "all":  # just import all from all files in dir: `data/`
		db_util.info()
		# db_util.clear_database()
		print("Trying to import all files from dir:", args.path)
		process_all_order_files(args.path, start_moment=dateparser.parse(args.timeshift))
		process_all_user_files(args.path, start_moment=dateparser.parse(args.timeshift))
		db_util.info()
		sys.exit()

	if args.mode == "simulate":  # simulte cronjob mode
		print("trying to simulate all files from dir", args.path)
		# ATTENTION !!! remove all data from orders and customer tables
		db_util.clear_database()
		db_util.info()
		now = datetime.datetime.now()
		begin = dateparser.parse("January 1th, 2020 00:00")
		sim_range = pd.date_range(start=begin, end=now, freq=args.freq)
		# sim_range = pd.date_range(start=begin, end=now, freq="5min")
		# good, bad = 0, 0
		with tqdm.tqdm(total=len(sim_range), desc="simulating time ") as pbar:
			for idx in range(len(sim_range)):
				moment = sim_range[idx]
				# moment_next = sim_range[idx + 1]
				scheduled_job(end_moment=moment)
				pbar.update(1)
				pbar.set_description("---===  %s" % str(moment) + "===---")
				pbar.refresh()
		db_util.info()

# Total records in DB.orders: 37313
# Total users in db.customers: 13926
# Empty users in db.customers: 5316
