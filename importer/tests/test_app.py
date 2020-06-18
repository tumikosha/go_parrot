from unittest import TestCase
from unittest import TestCase
import unittest


class Test(TestCase):
	def test_prepare_database(self):
		import db_util
		client, db = db_util.prepare_database()
		print(db)
		self.assertTrue(db is not None)

	def test_csv_2_df(self):
		import app
		# df = app.csv_2_df('../data/users_202002181303.csv', dtype=app.USERS_DTYPES)
		df = app.csv_2_df('../data/users_xxx.csv', dtype=app.USERS_DTYPES)
		# print(len(df))
		# print('>>>', df.iloc[0]['created_at'], type(df.iloc[0]['created_at']))
		self.assertEqual(8614, len(df))
		# self.assertTrue(True)


# self.fail()
if __name__ == '__main__':
	unittest.main()

