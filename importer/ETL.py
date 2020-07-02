# -*- coding: utf-8 -*-
"""
	ETL functions
"""
import pandas as pd
import entities as entity


def prepare_user(user) -> dict:
	"""prepare dataFrame row to write in MongoDB """
	user['_id'] = user['user_id']
	user = replace_nan(user, user.keys(), None)
	# user['is_empty'] = False
	if user.get('user_updated_at', None) is not None:
		user['updated_at'] = user.get('user_updated_at', None)
		del user['user_updated_at']

	return user


def prepare_order(order)-> dict:
	"""prepare dataFrame row to write in MongoDB """
	order['_id'] = order['id']
	order = replace_nan(order, order.keys(), None)
	return order


def prepare_full_order(full_order)-> dict:
	"""prepare dataFrame row to write in MongoDB """
	full_order['_id'] = full_order['id']
	full_order = replace_nan(full_order, full_order.keys(), None)
	return full_order


def prepare_users_df(df)-> pd.DataFrame:
	"""prepare dataFrame to write in MongoDB  """
	df['_id'] = df['user_id']
	return df


def is_user_correct(user) -> (bool, str):
	e_user = entity.User(user)
	e_user.have_errors()
	# check if subj is corect. incorrect users forwards to users_error_table
	if user.get('updated_at', None) is None:
		return False, "updated_at is None"
	return True, "Ok"


def is_order_correct(order) -> (bool, str):
	# check if subj is corect. incorrect orders forwards to orders_error_table
	if order.get('updated_at', None) is None:
		return False, "updated_at is None"
	if order['status'] is None:
		return False, "status is None"

	return True, "Ok"


def is_full_order_correct(full_order) -> (bool, str):
	# check if subj is corect. incorrect full_orders forwards to error_table
	# e_full_order = entity.FullOrder(full_order)
	e_full_order = entity.FullOrder.parse(full_order)
	is_err, reason = e_full_order.have_errors() # TODO parse all fields
	if is_err:
		return False, reason
	# if full_order['updated_at'] is None:
	# 	return False, "updated_at is None"
	# if full_order['user_updated_at'] is None:
	# 	return False, "user_updated_at is None"
	# if full_order['status'] is None:
	# 	return False, "status is None"
	# if full_order['status'] != full_order['status']: # means if status is `nan`
	# 	return False, "status is nan"

	return True, None


def NaT(df, column)-> pd.DataFrame:
	"""drop records with NaT values in column"""
	df[[column]] = df[[column]].astype(object).where(
		df[[column]].notnull(), None)

	# df[column] = df[column].astype(str)
	# df[column] = df[column].apply(lambda x : None if x=="NaT" else x)
	return df


def NaT_2_None(df, column)-> pd.DataFrame:
	""" """
	df[column] = df[column].astype(str)
	df[column] = df[column].apply(lambda x: None if x == "NaT" else x)
	return df


def NaT_2_None_all_cols(df)-> pd.DataFrame:
	for column in df.columns:
		print(column)
		try:
			df = NaT_2_None(df, column)
		except Exception as e:
			print("############", column, str(e))

	return df


def prepare_orders_df(df)-> pd.DataFrame:
	df = NaT_2_None_all_cols(df)
	return df


def replace_value(d, keys, func)-> dict:
	for key in keys:
		d[key] = func(d[key])
	return d


def replace_nan(d, keys, new_value)-> dict:
	return replace_value(d, keys, lambda value: new_value if value != value else value)


# d = {'a': float('nan'), 'b': float('nan'), 'c': float('nan')}
# print(d)
# print(replace_nan(d, ['a', 'b'], None))
