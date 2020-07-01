import app, tqdm, datetime, dateparser, sys
import pandas as pd
import pymongo

cfg_path = "config.yaml"
tip = "orders_mongo_source"
# start = dateparser.parse('1 sep 2019 00:00')
# end = dateparser.parse('1 nov 2019 00:00')
# start = dateparser.parse('1 years  and 1 month ago 00:00')
start = dateparser.parse('"16 months ago 00:00"')
end = dateparser.parse('today')

client = pymongo.MongoClient('mongodb://127.0.0.1:57017/admin')
db = client['src']
cursor = db.orders.find()
df = pd.DataFrame(list(cursor))
df_dup = df.drop_duplicates(subset=['updated_at'], keep=False)
# print(df_dup)

print(df)
sys.exit()

total = 0
for source_name, source, df in app.point_iterator(cfg_path, {}, start=start, end=end,
												  tip="orders_mongo_source", return_type='df'):

	print("Total:", len(df))
	total = len(df)
	print(df)

# for source_name, source, df in app.point_iterator(cfg_path, {}, start=start, end=end,
# 												  tip="users_mongo_source", return_type='df'):
# 	print("Total:", len(df))
# 	total = len(df)
# 	print(df)
#
sys.exit()

sim_range = pd.date_range(start=start, end=end, freq="1W", closed=None)
print("sim_range", sim_range)
total_size, total_y_size = 0, 0
with tqdm.tqdm(total=len(sim_range), desc="simulating time ") as pbar:
	for idx in range(len(sim_range) - 1):
		moment = sim_range[idx]
		moment_next = sim_range[idx + 1]
		for source_name, source, df in app.point_iterator(cfg_path, {},
														  start=moment, end=moment_next,
														  tip=tip, return_type='df'):
			size = 0
			if df is not None:
				size = len(df)
			print("size:", size)
			total_size += size
			y_size = app.process_yaml("config.yaml", start=moment, end=moment_next)
			total_y_size += y_size

print(total, total_size, total_y_size)
