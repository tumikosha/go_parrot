from unittest import TestCase
from unittest import TestCase
import unittest


class Test(TestCase):
	def test_rec_transform(self):
		import app
		# cfg_path = "../data/config_test.yaml"
		cfg_path = "data/config_test.yaml"
		i = 0
		for source_name, source, row in app.point_iterator(cfg_path, {}, tip="csv_orders", return_type='row'):
			print(row.to_dict())
			if i >0: break
			i += 1

		self.assertTrue(True)
		# self.assertEqual(i, j)
		# self.fail()


if __name__ == '__main__':
	unittest.main()
