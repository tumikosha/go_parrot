# import petl as etl, psycopg2 as pg, sys
import petl, os
import yaml
import sys
import pandas as pd
from pprint import pprint


def load_all_files(dir, start_with):
	order_files = [f for f in os.listdir(dir) if f.startswith(start_with)]
	count = 0
	for file in order_files:
		orders_ = petl.fromcsv(dir + "/" + file)
		if count == 0:
			all = orders_
		else:
			all = petl.merge(all, orders_, key="updated_at")
		count += 1
	return all


class SourcesIterator():
	CONNECTIONS = {}

	def __init__(self, yaml_path):
		with open(yaml_path) as f:
			config = yaml.safe_load(f)
		# pprint(config)
		self.sources = config['sources']
		# for source_name, source in self.sources.items():
		# 	print(source_name, source['type'])

	def sources_iterator(self, query, tip='csv_orders'):
		for source_name, source in self.sources.items():
			if source['type'] == tip:
				df = pd.read_csv(source['uri'], dtype=source['dtype'])
				print(len(df))
				for idx, row in df.T.iteritems():
					yield (source_name, source, row)


def point_iterator(yaml_path, query, tip='csv_orders'):
	with open(yaml_path) as f:
		config = yaml.safe_load(f)
	# self.sources = config['sources']
	for source_name, source in config['points'].items():
		if source['type'] == tip:
			df = pd.read_csv(source['uri'], dtype=source['dtype'])
			print(len(df))
			for idx, row in df.T.iteritems():
				yield (source_name, source, row)


if __name__ == "__main__":
	cfg_path= "config.yaml"

	for source_name, source, row in point_iterator(cfg_path, {}, tip="csv_orders"):
		print(source_name, dict(row))
	for source_name, source, row in point_iterator(cfg_path, {}, tip="csv_users"):
		print(source_name, dict(row))

	# print(source_name, source['type'], source['uri'], source['dtype'])
	# for source_name, source, row in conn_man.sources_iterator({}, tip="csv_users"):
	# 	print(source_name, dict(row))


	sys.exit()
	# orders = petl.fromcsv('data/orders_xxx.csv')
	# orders = petl.fromcsv('data/orders_202002181303.csv')
	orders = load_all_files('data', "orders")
	# users = petl.fromcsv('data/users_xxx.csv')
	# users = petl.fromcsv('data/users_202002181303.csv')
	users = load_all_files('data', "users")
	# orders = petl.fromdb(connection_a, 'SELECT * FROM orders')
	# users = petl.fromdb(connection_b, 'SELECT * FROM users')
	users = petl.rename(users, 'updated_at', "u_updated_at")
	print('total loaded orders:', len(orders))
	print('total loaded users:', len(users))
	# just for easy visualization
	# orders = etl.cutout(orders, "date_tz", "item_count", "order_id", "receive_method", "status", "store_id", "subtotal", \
	# 					"tax_percentage", "total", "total_discount", "total_gratuity", "total_tax",
	# 					"fulfillment_date_tz")
	users = petl.cutout(users, "merchant_id", "phone_number", "created_at")
	users = petl.groupselectmax(users, 'user_id', 'u_updated_at')  # select only fresh data
	orders = petl.groupselectmax(orders, 'id', 'updated_at')  # select only fresh data
	print(users)
	full_orders = petl.join(orders, users, key='user_id', presorted=True)
	print(full_orders)
	conflicts = full_orders.select(lambda rec: str(rec['id']).startswith("Confl"))
	print("conflicts: ", conflicts)
	print("conflicts: ", len(conflicts))
	petl.tocsv(full_orders, 'data/full_orders.csv')
# petl.toxlsx(full_orders, "data/full_orders.xlsx")
# petl.todb(table, dbo, tablename, schema=None, commit=True)
# petl.tosqlite3(full_orders, 'full_orders.db', 'full_orders') # check version of PETL
