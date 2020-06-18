from unittest import TestCase
import unittest


class Test(TestCase):
	def test_prepare_database(self):
		import db_util
		client, db = db_util.prepare_database()
		print(db)
		self.assertTrue(db is not None)
		# self.assertTrue(True)


if __name__ == '__main__':
	unittest.main()