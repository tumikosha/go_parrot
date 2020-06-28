"""
	ETL functions
"""


def prepare_user(user):
	user['_id'] = user['user_id']
	user['is_empty'] = False
	if user.get('user_updated_at', None) is not None:
		user['updated_at'] = user.get('user_updated_at', None)
		del user['user_updated_at']
	return user


def prepare_order(order):
	order['_id'] = order['id']
	return order


def prepare_users_df(df):
	df['_id'] = df['user_id']
	return df


def is_order_correct(order) -> bool:
	if order['updated_at'] is None:
		return False

	return True


def is_user_correct(user) -> bool:
	if user['updated_at'] is None:
		return False

	return True