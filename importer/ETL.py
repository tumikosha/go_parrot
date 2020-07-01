"""
	ETL functions
"""


def prepare_user(user):
	user['_id'] = user['user_id']
	user = replace_nan(user, user.keys(), None)
	# user['is_empty'] = False
	if user.get('user_updated_at', None) is not None:
		user['updated_at'] = user.get('user_updated_at', None)
		del user['user_updated_at']

	return user


def prepare_order(order):
	order['_id'] = order['id']
	order = replace_nan(order, order.keys(), None)
	return order


def prepare_full_order(full_order):
	full_order['_id'] = full_order['id']
	full_order = replace_nan(full_order, full_order.keys(), None)
	return full_order


def prepare_users_df(df):
	df['_id'] = df['user_id']
	return df


# def is_order_correct(order) -> bool:
# 	if order['updated_at'] is None:
# 		return False
#
# 	return True


def is_user_correct(user) -> (bool, str):
	# check if subj is corect
	if user.get('updated_at', None) is None:
		return False, "updated_at is None"
	return True, "Ok"


def is_order_correct(order) -> (bool, str):
	# check if subj is corect
	if order.get('updated_at', None) is None:
		return False, "updated_at is None"
	if order['status'] is None:
		return False, "status is None"

	return True, "Ok"


def is_full_order_correct(full_order) -> (bool, str):
	# check if subj is corect
	if full_order['updated_at'] is None:
		return False, "updated_at is None"
	if full_order['user_updated_at'] is None:
		return False, "user_updated_at is None"
	if full_order['status'] is None:
		return False, "status is None"
	if full_order['status'] != full_order['status']: # means if status is `nan`
		return False, "status is nan"

	return True, "Ok"


def NaT(df, column):
	"""drop records with NaT values in column"""
	df[[column]] = df[[column]].astype(object).where(
		df[[column]].notnull(), None)

	# df[column] = df[column].astype(str)
	# df[column] = df[column].apply(lambda x : None if x=="NaT" else x)
	return df


def NaT_2_None(df, column):
	""" """
	df[column] = df[column].astype(str)
	df[column] = df[column].apply(lambda x: None if x == "NaT" else x)
	return df


def NaT_2_None_all_cols(df):
	for column in df.columns:
		print(column)
		try:
			df = NaT_2_None(df, column)
		except Exception as e:
			print("############", column, str(e))

	return df


def prepare_orders_df(df):
	df = NaT_2_None_all_cols(df)
	return df


def replace_value(d, keys, func):
	for key in keys:
		d[key] = func(d[key])
	return d


def replace_nan(d, keys, new_value):
	return replace_value(d, keys, lambda value: new_value if value != value else value)


# d = {'a': float('nan'), 'b': float('nan'), 'c': float('nan')}
# print(d)
# print(replace_nan(d, ['a', 'b'], None))
