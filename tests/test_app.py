from unittest import TestCase


class Test(TestCase):
	def test_csv_2_df(self):
		import app
		# df = app.csv_2_df('../data/users_202002181303.csv', dtype=app.USERS_DTYPES)
		df = app.csv_2_df('../data/users_xxx.csv', dtype=app.USERS_DTYPES)
		print('>>>', df.iloc[0]['created_at'], type(df.iloc[0]['created_at']))
		self.assertTrue(True)
# self.fail()
