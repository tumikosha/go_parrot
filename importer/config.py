import datetime
import dateparser


MONGO_URI = 'mongodb://127.0.0.1:57017/admin'
DB_NAME = 'go_parrot'

# MONGO_URI = 'mongodb://db_name:pass@127.0.0.1:57017/admin'

# In event based systems, the order of events may be disturbed in a small range (1 min?)
# this parameter solves this problem
# Now it usually captures 1 (or few) last record, but does not write it to the database.

# TIME_DELTA = datetime.datetime.now() - dateparser.parse("1 minute ago")
# TIME_DELTA = datetime.datetime.now() - dateparser.parse("1 day ago")
# see https://dateparser.readthedocs.io/en/latest/#relative-dates

TIME_DELTA = datetime.timedelta(days=0, seconds=0, microseconds=0, milliseconds=0,
								minutes=0, hours=0, weeks=0)
