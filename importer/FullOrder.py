"""
	Your mission will be to create an ETL service in python to transfer data from a production database to another data warehouse.
	Attached you can find 2 CSV files (users and orders). These files simulate the production database data. Download the CSV files and insert them into a local database of your choice (for example MongoDB).
	Create a python service with a cron job that runs every 5 min (preferably using flask with flask cron).
	The purpose of this cron job is to extract updated orders records (along with their user information) from the production database and push them into the data warehouse (Postgres DB) into 1 table.
	Imagine that today is January 1th, 2020. You need to quickly synchronize all previous orders before today and after this day use the cron job to synchronize new orders. You will need to simulate the clock moving forward by 5 min every time the cron job starts to work. So time on the first run of the cron job will be 01/01/2020 00:00. And next time cron job runs it will be 01/01/2020 00:05 and so on.
	Note that the updated_at column holds the date and time of the last update of a record.
	All code should be on github.com and should be easy to install for the developer.
	-----------------------------
	Dear Veaceslav,

	here please find some suggestions for the test task and please try to redo it.


	The purpose of the test task is to create an ETL service which basically means extracting data from multiple sources,
	The attached CSV files were sent as example of the data which should be loaded into a database ( manual loading or
	automated is a matter of choice and time).
	The service should extract the data (take into consideration handling big amount of data) from the database, process it,
	 and store into a single destination table.
	Hint: The service should be error-proof and should be able to reindex all the missing data in case of an error or in
	the case if it was not started by the cron job.

"""
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
	def have_errors(self) -> bool:
		return False

	pass


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
