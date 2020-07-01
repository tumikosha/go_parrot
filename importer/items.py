import json


class Record:
	def __init__(self, row_dict):
		for key in row_dict:
			self.__dict__[key] = row_dict[key]

	def have_errors(self) -> bool:
		return False

	def toJSON(self) -> str:
		return json.dumps(self, default=lambda o: o.__dict__, sort_keys=True, indent=4)


class Order(Record):
	def have_errors(self) -> bool:
		return False


class User(Record):
	user_id = None
	first_name = None
	last_name = None
	merchant_id = None
	phone_number = None
	created_at = None
	updated_at = None


	def have_errors(self) -> bool:
		# def is_user_correct(user) -> (bool, str):
		if self.updated_at is None:
			return True
		return False




class FullOrder(Order):
	user: User = None

	def set_user(self, user):
		self.user = user

	def fill_user_info(self):
		pass

	def update_with_new_data(self):
		pass


if __name__ == "__main__":
	full_order = FullOrder({'id': '2222', 'item': 'Pizza'});
	full_order.set_user({'user_id': '111', 'name': 'Vasia'})
	print(full_order.toJSON())

# Order
# "id", "created_at", "date_tz", "item_count", "order_id", "receive_method",
# "status", "store_id", "subtotal", "tax_percentage", "total", "total_discount",\
# "total_gratuity", "total_tax", "updated_at", "user_id", "fulfillment_date_tz"

# User
# "user_id","first_name","last_name","merchant_id","phone_number","created_at","updated_at"
