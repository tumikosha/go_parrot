from unittest import TestCase
from unittest import TestCase
import unittest


class Test(TestCase):
	def test_prepare_database(self):
		import db_util
		client, db = db_util.prepare_database()
		print(db)
		self.assertTrue(db is not None)

	def test_point_orders_iterators(self):
		import app
		cfg_path = "data/config_test.yaml"
		# cfg_path = "../data/config_test.yaml"
		i = 0
		for source_name, source, point_type in app.point_iterator(cfg_path, {}, tip="csv_orders", return_type='point'):
			i += 1
		self.assertEqual(i, 2)

	def test_point_users_iterators(self):
		import app
		cfg_path = "data/config_test.yaml"
		# cfg_path = "../data/config_test.yaml"
		i = 0
		for source_name, source, point_type in app.point_iterator(cfg_path, {}, tip="csv_orders", return_type='point'):
			i += 1
		self.assertEqual(i, 2)

	def test_df_users_iterators(self):
		import app
		# cfg_path = "../data/config_test.yaml"
		cfg_path = "data/config_test.yaml"
		i = 0
		for source_name, source, df in app.point_iterator(cfg_path, {}, tip="csv_orders", return_type='df'):
			i += 1
		self.assertEqual(i, 2)

	# self.fail()

	def test_df_users_iterators(self):
		import app
		# cfg_path = "../data/config_test.yaml"
		cfg_path = "data/config_test.yaml"
		j = 0
		for source_name, source, df in app.point_iterator(cfg_path, {}, tip="csv_orders", return_type='df'):
			j += len(df)

		i = 0
		for source_name, source, row in app.point_iterator(cfg_path, {}, tip="csv_orders", return_type='row'):
			i += 1

		self.assertTrue((i > 30000) and i==j)
		# self.assertEqual(i, j)

	def test_rec_transform(self):
		import app
		# cfg_path = "../data/config_test.yaml"
		cfg_path = "data/config_test.yaml"
		i = 0
		for source_name, source, row in app.point_iterator(cfg_path, {}, tip="csv_orders", return_type='row'):
			print(row)
			if i >0: break
			i += 1

		self.assertTrue((i > 30000) and i==j)
		# self.assertEqual(i, j)
		# self.fail()


if __name__ == '__main__':
	unittest.main()
