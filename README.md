This script imports all files with orders_xxxx.csv/users_xxxx.csv from directory `data/`

# import all files  from directory:
	python app.py --mode all --path data/ --dest mongodb://127.0.0.1:57017/admin
	
# Simulate 5min activity:
	python app.py --mode simulate --freq 5min --dest mongodb://127.0.0.1:57017/admin		
	
# Run service by cron:
	flask crontab add

# Requirements
Anaconda / Python 3.7.4

Mongodb 4  on port 57017 is required and started in docker container in `run.sh`

App creates database `go_parrot`

    `go_parrot.order` - table/collection with orders
    
    `go_parrot.customers`  - table/collection with users
    
    go_parrot.customers['is_empty'] = True if order refers to user without description
    
tables `related` by go_parrot.order[user_id] -> go_parrot.customers[_id]

cron service scans `data/` every 5min and try to import only records with

    `updated_at` > max(updated_at) - TIME_DELTA
    
    where  TIME_DELTA- configurable parameter (0 sec by default)
    
            max(updated_at) - calculated from db

In event based systems, the order of events may be disturbed in a small range (1 min?)

TIME_DELTA parameter solves this problem

in code it looks like:
```
    filter = (df['updated_at'] > (start_moment - config.TIME_DELTA)) & (df['updated_at'] <= end_moment) 
```
If it is not 0 it usually captures 1 (or few) last record, and tried to replace them in the database.

 see TIME_DELTA in config.py 
 
 
----------------------------------------------------------------

optional arguments:

  -h, --help            show this help message and exit
  
  -d DEST, --dest DEST  mongodb URI like  
                        mongodb://db_name:password@ip:port/admin
                        
  -p PATH, --path PATH  path to dir with files
  
  -m {all,simulate},    --mode {all,simulate}
  
        import all files or simulate
                        
  -f FREQ, --freq FREQ 
   
   cron freq for simulating ex: 5min 12h 1M
  

# Available freq
    Alias	Description
    B	business day frequency
    C	custom business day frequency
    D	calendar day frequency
    W	weekly frequency
    M	month end frequency
    SM	semi-month end frequency (15th and end of month)
    BM	business month end frequency
    CBM	custom business month end frequency
    MS	month start frequency
    SMS	semi-month start frequency (1st and 15th)
    BMS	business month start frequency
    CBMS	custom business month start frequency
    Q	quarter end frequency
    BQ	business quarter end frequency
    QS	quarter start frequency
    BQS	business quarter start frequency
    A, Y	year end frequency
    BA, BY	business year end frequency
    AS, YS	year start frequency
    BAS, BYS	business year start frequency
    BH	business hour frequency
    H	hourly frequency
    T, min	minutely frequency
    S	secondly frequency
    L, ms	milliseconds
    U, us	microseconds
    N	nanoseconds