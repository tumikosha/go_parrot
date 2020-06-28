import pandas as pd
import datetime


def csv_2_df(path, dtype=None):
	"""load csv to pandas dataframe """
	f = lambda s: datetime.datetime.strptime(s, '%Y-%m-%d %H:%M:%S')
	df_ = pd.read_csv(path, date_parser=f, dtype=dtype,
					  parse_dates=['created_at', 'updated_at'])
	return df_

# np.unique(df[['column1', 'column2']].values)
	# df['col_1'].unique()

f = lambda s: datetime.datetime.strptime(s, '%Y-%m-%d %H:%M:%S')
df = csv_2_df("../data/users_xxx.csv")
print(df)
grouped_df = df.groupby("user_id", as_index=False)
maximums = grouped_df['updated_at'].max()
# maximums = maximums.reset_index()
print(maximums)