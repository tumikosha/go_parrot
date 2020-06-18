import datetime
import dateparser

MONGO_URI = 'mongodb://127.0.0.1:57017/admin'
# MONGO_URI = 'mongodb://db_name:pass@127.0.0.1:57017/admin'

TIME_DELTA = datetime.datetime.now() - dateparser.parse("1 minute ago")