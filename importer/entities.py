import json
import dateparser
from pprint import pprint

order_1 = {
	'_id': "111111==2019-03-01 00:00:00",
	'id': "111111",
	'created_at': dateparser.parse('1 year ago'),
	'date_tz': None,
	'item_count': 1,
	'order_id': "5e171d321fa1160034e32ccc",
	'receive_method': "pickup1111",
	'status': None,
	'store_id': "f2c88008-8fd0-4529-9c5a-bd76518c2649",
	'subtotal': 10.85,
	'tax_percentage': 0.08875,
	'total': 11.81,
	'total_discount': 0,
	'total_gratuity': 0,
	'total_tax': 0.96,
	'updated_at': dateparser.parse('1 year ago'),
	'user_id': "000042908946867",
	'fulfillment_date_tz': None,
	'user_first_name': 'Vasilisa',
	'user_last_name': 'Obada',

}
full_order_1 = order_1.copy()


def class_vars(c):
	return [attr for attr in dir(c) if not callable(getattr(c, attr)) and not attr.startswith("__")]


class Record:
	def __init__(self, row_dict):
		for key in row_dict:
			self.__dict__[key] = row_dict[key]

	def have_errors(self) -> bool:
		return False

	def toJSON(self) -> str:
		import datetime
		# f = lambda o: o.__dict__
		f = lambda o: str(o) if isinstance(o, datetime.datetime) else o.__dict__
		return json.dumps(self, default=f, sort_keys=True, indent=4)

	def by_prefix(prefix, row):
		d, r = {}, {}
		for key in row.keys():
			if key.startswith(prefix):
				d[key[len(prefix):]] = row[key]
			# row.pop(key, None)
			else:
				r[key] = row[key]
		return d, r

	def to_row(self, prefix="") -> str:
		d = self.__dict__.copy()
		d2 = {}
		for k in d:
			d2[prefix + k] = d[k]
		return d2


class Order(Record):
	def have_errors(self) -> bool:
		return False


class User(Record):
	# fields = ["user_id", "first_name", "last_name", "merchant_id", "phone_number", "created_at", "updated_at"]

	def __init__(self, row_dict):
		super().__init__(row_dict)

	def have_errors(self) -> bool:
		# def is_user_correct(user) -> (bool, str):
		if self.__dict__.get('updated_at', None) is None:
			return True
		if self.__dict__.get('created_at', None) is None:
			return True
		return False

	def to_row(self, prefix="user_") -> str:
		return super().to_row(prefix="user_")

	def to_row_JSON(self) -> str:
		return json.dumps(self.to_row(), default=lambda o: o.__dict__, sort_keys=True, indent=4)


class FullOrder(Order):
	user: User = None

	def __init__(self, row_dict):
		user_dict, order_dict = Record.by_prefix('user_', row_dict)
		self.user = User(user_dict)
		super().__init__(order_dict)

	def set_user(self, user):
		# for key in user.__dict__:
		# 	self.__dict__[key] = user.__dict__[key]
		self.user = user

	def from_row(self, row):
		d = {}
		for key in row.keys():
			if key.startswith('user_'):
				d[key] = row[key]
				row.pop(key, None)
		self.user = User(d)
		return d

	def fill_user_info(self):
		pass

	def update_with_new_data(self):
		pass

	def to_row(self, prefix="user_") -> str:
		user_row = self.user.to_row()
		d = self.__dict__.copy()
		d.update(user_row)
		d.pop('user', None)
		return d

	# return super().to_row(prefix="user_")

	def to_row_JSON(self) -> str:
		d = self.to_row()
		d.pop('user', None)
		return json.dumps(d, default=lambda o: o.__dict__, sort_keys=True, indent=4)


if __name__ == "__main__":
	# u_dict,o_dict =  Record.by_prefix("user_", full_order_1)
	# print(u_dict)
	# print(o_dict)
	# user = User({'first_name': "Gibon"})
	# print(user.to_row())
	full_order = FullOrder(full_order_1)
	print(full_order.toJSON())
	pprint(full_order.to_row())
