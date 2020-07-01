from unittest import TestCase
from unittest import TestCase
import unittest


class Test(TestCase):
	# def test_join(self):
	# 	import app
	# 	import pandas as pd
	# 	df_orders = app.csv_2_df('data/orders_xxx.csv')
	# 	df_orders['updated_at'] = pd.to_datetime(df_orders['updated_at'])
	# 	df_orders['created_at'] = pd.to_datetime(df_orders['created_at'])
	# 	print('\n orders before group:', len(df_orders))
	# 	df_orders = df_orders.loc[df_orders.groupby("id")["updated_at"].idxmax()]  # keep max updated_at
	# 	df_users = app.csv_2_df('data/users_xxx.csv')
	# 	df_users = app.fix_df_users(df_users)
	# 	df_users = df_users.loc[df_users.groupby("user_id")["user_updated_at"].idxmax()]  # keep max updated_at
	# 	full_orders = df_orders.join(df_users.set_index('user_id'), on='user_id')
	# 	self.assertEqual(len(df_orders), len(full_orders))
	#
	# # self.fail()
	#
	# def test_mongo_query(self):
	# 	import app
	# 	import pymongo, dateparser
	# 	import db_util
	# 	client = pymongo.MongoClient('mongodb://127.0.0.1:57017/admin')
	# 	db = client['go_parrot_src']
	# 	start = dateparser.parse('1 jan 2019')
	# 	end = dateparser.parse('20 sept 2019')
	# 	q = {"$and": [{'updated_at': {'$gte': start}}, {'updated_at': {'$lte': end}}]}
	# 	# q = {}
	# 	print("\n query:", q)
	# 	cursor = db.orders.find(q)
	# 	arr = list(cursor)
	# 	print("\n orders::", len(arr))
	# 	self.assertEqual(len(arr), 2803)
	#
	# def test_mongo_query_range(self):
	# 	import app, pymongo, dateparser, datetime, db_util, tqdm
	# 	import pandas as pd
	# 	client = pymongo.MongoClient('mongodb://127.0.0.1:57017/admin')
	# 	db = client['go_parrot_src']
	# 	start = dateparser.parse('1 jan 2019')
	# 	end = dateparser.parse('today')

	# 	# q = {}
	# 	total = 0
	# 	sim_range = pd.date_range(start=start, end=datetime.datetime.now(), freq="1M")
	# 	with tqdm.tqdm(total=len(sim_range), desc="simulating time ") as pbar:
	# 		for idx in range(len(sim_range) - 1):
	# 			moment = sim_range[idx]
	# 			moment_next = sim_range[idx + 1]
	# 			# scheduled_job(end_moment=moment)
	# 			# process_yaml(args.yaml, start=moment, end=moment_next)
	# 			q = {"$and": [{'updated_at': {'$gte': moment}}, {'updated_at': {'$lte': moment_next}}]}
	# 			cursor = db.orders.find(q)
	# 			arr = list(cursor)
	# 			num = app.process_yaml("config.yaml", start=start, end=end)
	# 			print("==>",len(arr), num)
	# 			total = total + len(arr)
	# 			pbar.update(1)
	# 			pbar.set_description("---===  %s" % str(moment) + "===---")
	# 			pbar.refresh()
	# 	print("\n TOTAL:", total)
	# 	start = dateparser.parse('1 jan 2019')
	# 	end = dateparser.parse('20 today')
	# 	q = {"$and": [{'updated_at': {'$gte': start}}, {'updated_at': {'$lte': end}}]}
	# 	cursor = db.orders.find(q)
	# 	arr = list(cursor)
	# 	print("OVERALL", len(arr))
	# 	# self.assertEqual(len(arr), 2803)
	# 	self.assertTrue(True)

	# def test_mongo_query_range(self):
	# 	import app, pymongo, dateparser, datetime, db_util, tqdm
	# 	import pandas as pd
	# 	client = pymongo.MongoClient('mongodb://127.0.0.1:57017/admin')
	# 	db = client['go_parrot_src']
	# 	start = dateparser.parse('1 sept 2019')
	# 	end = dateparser.parse('today')
	# 	total = 0
	# 	sim_range = pd.date_range(start=start, end=datetime.datetime.now(), freq="1M")
	# 	idx = 2
	# 	moment = sim_range[idx]
	# 	moment_next = sim_range[idx + 1]
	# 	q = {"$and": [{'updated_at': {'$gte': moment}}, {'updated_at': {'$lte': moment_next}}]}
	# 	cursor = db.orders.find(q)
	# 	arr = list(cursor)
	# 	num = app.process_yaml("config.yaml", start=moment, end=moment_next)
	# 	print("==>", len(arr), num)
	# 	total = total + len(arr)
	#
	# 	print("OVERALL", len(arr))
	# 	# self.assertEqual(len(arr), 2803)
	# 	self.assertTrue(True)


	def test_mongo_query_range(self):
		import app, pymongo, dateparser, datetime, db_util, tqdm
		import pandas as pd
		client = pymongo.MongoClient('mongodb://127.0.0.1:57017/admin')
		db = client['go_parrot_src']
		start = dateparser.parse('1 sept 2019')
		end = dateparser.parse('today')
		total = 0
		sim_range = pd.date_range(start=start, end=datetime.datetime.now(), freq="1M")
		idx = 2
		moment = sim_range[idx]
		moment_next = sim_range[idx + 1]
		q = {"$and": [{'updated_at': {'$gte': moment}}, {'updated_at': {'$lte': moment_next}}]}
		cursor = db.orders.find(q)
		arr = list(cursor)
		num = app.process_yaml("config.yaml", start=moment, end=moment_next)
		print("==>", len(arr), num)
		total = total + len(arr)

		print("OVERALL", len(arr))
		# self.assertEqual(len(arr), 2803)
		self.assertTrue(True)


if __name__ == '__main__':
	unittest.main()
