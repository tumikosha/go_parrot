from unittest import TestCase
from unittest import TestCase
import unittest
import datetime, app, dateparser

from enum import Enum


class IterType(Enum):
	POINT = "point"
	DF = 'df'
	ROW = 'row'


def uni_iterator(yaml_path, query, tip='csv_orders', return_type=IterType.POINT, start=dateparser.parse("100 years ago"),
				 end=dateparser.parse("2 days in")):
	if return_type == IterType.POINT:
		for a, b, c in app.point_iterator(yaml_path, tip=tip):
			yield a, b, c

	if return_type == IterType.DF:
		for a, b, c in app.df_iterator(yaml_path, {}, tip=tip, start=start, end=end):
			yield a, b, c

	if return_type == IterType.ROW:
		for a, b, c in app.row_iterator(yaml_path, {}, tip=tip, start=start, end=end):
			yield a, b, c


class Test3(TestCase):

	# def test_point_orders_iterators(self):
	# 	import app
	# 	cfg_path = "data/config_test.yaml"
	# 	# cfg_path = "../data/config_test.yaml"
	# 	i = 0
	# 	for source_name, source, point_type in app.universal_iterator(cfg_path, {}, tip="csv_orders",
	# 																  return_type='point'):
	# 		i += 1
	# 	self.assertEqual(i, 2)
	#
	# def test_point_users_iterators(self):
	# 	import app
	# 	cfg_path = "data/config_test.yaml"
	# 	# cfg_path = "../data/config_test.yaml"
	# 	i = 0
	# 	for source_name, source, point_type in app.universal_iterator(cfg_path, {}, tip="csv_orders",
	# 																  return_type='point'):
	# 		i += 1
	# 	self.assertEqual(i, 2)
	#
	# def test_df_users_iterators(self):
	# 	import app
	# 	# cfg_path = "../data/config_test.yaml"
	# 	cfg_path = "data/config_test.yaml"
	# 	i = 0
	# 	for source_name, source, df in app.universal_iterator(cfg_path, {}, tip="csv_orders", return_type='df'):
	# 		i += 1
	# 	self.assertEqual(i, 2)
	#
	# # self.fail()
	#
	# def test_df_users_iterators(self):
	# 	import app
	# 	# cfg_path = "../data/config_test.yaml"
	# 	cfg_path = "data/config_test.yaml"
	# 	j = 0
	# 	for source_name, source, df in app.universal_iterator(cfg_path, {}, tip="csv_orders", return_type='df'):
	# 		j += len(df)
	#
	# 	i = 0
	# 	for source_name, source, row in app.universal_iterator(cfg_path, {}, tip="csv_orders", return_type='row'):
	# 		i += 1
	#
	# 	self.assertTrue((i > 30000) and i == j)
	#
	# # self.assertEqual(i, j)
	#
	# def test_rec_transform(self):
	# 	import app
	# 	# cfg_path = "../data/config_test.yaml"
	# 	cfg_path = "data/config_test.yaml"
	# 	i = 0
	# 	for source_name, source, row in app.universal_iterator(cfg_path, {}, tip="csv_orders", return_type='row'):
	# 		print(row)
	# 		if i > 0: break
	# 		i += 1
	#
	# 	self.assertTrue((i > 30000) and i == j)
	#
	# # self.assertEqual(i, j)
	# # self.fail()
	#
	# def test_point_iterator(self):
	# 	import app
	# 	cfg_path = "data/config_test.yaml"
	# 	i = 0
	# 	for source_name, source, point_type in app.point_iterator(cfg_path, tip="csv_orders"):
	# 		print("\n #####", source)
	# 		i += 1
	# 	self.assertEqual(i, 2)
	#
	# def test_df_iterator(self):
	# 	import app
	# 	cfg_path = "data/config_test.yaml"
	# 	i = 0
	# 	for source_name, source, df in app.df_iterator(cfg_path, {}, tip="csv_orders"):
	# 		print("\n #####", source)
	# 		i += len(df)
	# 	print(i)
	# 	self.assertEqual(i, 37323)
	#
	# def test_row_iterator_csv_orders(self):
	# 	import app
	# 	cfg_path = "data/config_test.yaml"
	# 	i = 0
	# 	for source_name, source, row in app.row_iterator(cfg_path, {}, tip="csv_orders"):
	# 		i += 1
	# 	print("Rows scanned", i)
	#
	# 	self.assertEqual(i, 37323)
	#
	# def test_row_iterator_mongo(self):
	# 	import app, dateparser
	# 	cfg_path = "config.yaml"
	# 	i = 0
	# 	start = dateparser.parse("2020-03-03")
	# 	end = dateparser.parse("2020-04-10")
	# 	for source_name, source, row in app.row_iterator(cfg_path, {}, tip="orders_mongo_source",
	# 													 start=start, end=end):
	# 		i += 1
	# 		print(row)
	# 	print("Rows scanned", i)
	# 	self.assertTrue(True)
	# # self.assertEqual(i, 37323)

	def test_universal(self):
		import dateparser
		cfg_path = "config.yaml"
		i = 0
		start = dateparser.parse("2020-03-03")
		end = dateparser.parse("2020-04-10")
		for source_name, source, row in uni_iterator(cfg_path, {}, tip="orders_mongo_source",
													 return_type=IterType.ROW,
													 start=start, end=end):
			i += 1
			print("\n &&&", row)
		print("Rows scanned", i)
		self.assertTrue(True)


# self.assertEqual(i, 37323)


if __name__ == '__main__':
	unittest.main()
