import json
import dateparser
from pprint import pprint
import config
import ETL

order_1 = {
	'_id': "111111==2019-03-01 00:00:00$$$$$$$",
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
	'first_name': 'Vasilisa',
	'last_name': 'Obada',
	'user_updated_at': '20 jan 2020',
	'user_created_at': '20 jan 2021',

}
full_order_1 = order_1.copy()


def class_vars(c):
	return [attr for attr in dir(c) if not callable(getattr(c, attr)) and not attr.startswith("__")]


class Record:
	def __init__(self, row_dict):
		row_dict = ETL.replace_nan(row_dict, row_dict.keys(), None)
		self.__dict__['_id'] = row_dict.get('id', None)
		for key in row_dict:
			self.__dict__[key] = row_dict[key]
		# replace_nan(full_order, full_order.keys(), None)

	def have_errors(self) -> bool:
		return False

	def set(self, key, value):
		self.__dict__[key] = value

	def get(self, key, default_value):
		return self.__dict__.get(key, default_value)

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

	def by_key_list(key_arr, row):
		d, r = {}, {}
		for key in row.keys():
			if key in key_arr:
				d[key] = row[key]
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

	def update(self, point, db=None, table=None):
		"""
		Check if it is a fresh order and write it to db
		:param record: dictionary with fields after ETL
		:return: STATUS_INSERTED | STATUS_REPLACED | STATUS_SKIPPED
		"""
		try:
			# record = ETL.replace_nan(record, record.keys(), None)  # ??
			old_record = db[table].find_one({'_id': self.__dict__.get('_id', None)})
			if old_record is None:
				db[table].insert_one(self.to_row())
				# this is absolutely new order
				return config.STATUS_INSERTED

			if old_record['updated_at'] is None:
				old_record['updated_at'] = config.VERY_EARLY_DATE  # protect from None instead of date

			if old_record.get('updated_at', config.VERY_EARLY_DATE) < self.__dict__.get('updated_at', config.VERY_EARLY_DATE):
				# overwrite if current record is later
				db[table].save(self.to_row())
				return config.STATUS_REPLACED
		# if we are here, the order is old and no need to update
		except Exception as e:
			print(str(e))

		return config.STATUS_SKIPPED


class Order(Record):
	def have_errors(self) -> (bool, str):
		if self.__dict__.get('updated_at', None) is None:
			return True, "updated_at is None"
		if self.__dict__.get('created_at', None) is None:
			return True, "created_at is None"
		if self.__dict__.get('status', None) is None:
			return True, "status is None"

		return False, None


class User(Record):
	fields = ["user_id", "first_name", "last_name", "merchant_id", "phone_number", "created_at", "updated_at"]

	def __init__(self, row_dict):
		super().__init__(row_dict)

	def have_errors(self) -> bool:
		# def is_user_correct(user) -> (bool, str):
		if self.__dict__.get('updated_at', None) is None:
			return True, "updated_at"
		if self.__dict__.get('created_at', None) is None:
			return True, "created_at"
		return False, None

	def to_row(self, prefix="user_") -> str:
		return super().to_row(prefix=prefix)

	def to_row_JSON(self) -> str:
		return json.dumps(self.to_row(), default=lambda o: o.__dict__, sort_keys=True, indent=4)


class FullOrder(Order):
	user: User = None

	def __init__(self, row_dict):
		user_dict, order_dict = Record.by_prefix('user_', row_dict)
		self.user = User(user_dict)
		super().__init__(order_dict)
		# df_orders_unfiltered['_id'] = df_orders_unfiltered['id'] + "==" + df_orders_unfiltered[
		# 				'updated_at'].astype(str)
		#self.__dict__['_id'] = row_dict.get('id', None) + "==" + str(row_dict.get('updated_at', None))

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

	def have_errors(self) -> (bool, str):
		# def is_user_correct(user) -> (bool, str):
		if self.__dict__.get('updated_at', None) is None:
			return True, "updated_at is None"
		if self.__dict__.get('created_at', None) is None:
			return True, "created_at is None"
		if self.__dict__.get('status', None) is None:
			return True, "status is None"

		return False, None

	def fill_user_info(self):
		pass

	def update_with_new_data(self):
		pass

	def to_row(self, prefix="user_") -> str:
		user_row = self.user.to_row()
		d = self.__dict__.copy()
		user_row.pop("user__id", None)
		d.update(user_row)
		d.pop('user', None)
		return d

	def to_row_JSON(self) -> str:
		d = self.to_row()
		d.pop('user', None)
		return json.dumps(d, default=lambda o: o.__dict__, sort_keys=True, indent=4)

	def parse(full_order):
		u_dict, o_dict = Record.by_prefix("user_", full_order)
		fields = ["user_id", "first_name", "last_name", "merchant_id", "phone_number"]
		u_dict2, o_dict2 = Record.by_key_list(fields, o_dict)
		e_full_order = FullOrder(o_dict2)
		d = {}
		d.update(u_dict2)
		d.update(u_dict)
		e_full_order.set_user(User(d))
		return e_full_order


if __name__ == "__main__":
	# u_dict, o_dict = Record.by_prefix("user_", full_order_1)
	# fields = ["user_id", "first_name", "last_name", "merchant_id", "phone_number"]
	# u_dict2, o_dict2 = Record.by_key_list(fields, o_dict)
	# print(u_dict)
	# print(o_dict2)
	fo = FullOrder.parse(full_order_1)
	upd = fo.get("updated_at", "")
	# upd = fo.get("user_id", "")
	print(upd)
	print(fo.toJSON())
# print(o_dict)
# user = User({'first_name': "Gibon"})
# print(user.to_row())
# full_order = FullOrder(full_order_1)
# print(full_order.toJSON())
# pprint(full_order.to_row())
