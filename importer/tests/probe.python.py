import pandas as pd


df = pd.read_csv(source_['uri'], dtype=source_['dtype'], index_col=False,v
							 date_parser=f, parse_dates=['created_at', 'updated_at'])