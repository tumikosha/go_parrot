This script imports all files with orders_xxxx.csv/users_xxxx.csv from directory `data/`
This script imports all files with orders_xxxx.csv/users_xxxx.csv from directory `data/`

 
# import all files  from directory:
```
    python app.py --mode all
    python app.py --start January_12,_2012_10:00   --end January_1,_2020_10:00 
    python app.py --mode all --path data/ --start 100_years_ago --end tomorrow --dest mongodb://127.0.0.1:57017/admin
```
	
# Simulate 5min activity:
	python app.py --mode simulate --freq 5min --dest mongodb://127.0.0.1:57017/admin		
	
# Run service by cron:
    cd importer
	flask crontab add
	
# Run service by PM2:	
    However, instead of cron, I recommend using the pm2 utility in combination with relative dates and avoid the flask-cron    
    This will keep control of the command line and memory and cron too
    It will allow you to forget about cron and make the task much easier.
    PM2 is a daemon process manager that will help you manage and keep your application online 24/7
    https://pm2.keymetrics.io/
     [ [pm2.keymetrics.io](https://pm2.keymetrics.io) ]
    
    pm2 start 'python app.py --mode all ' --name app --cron "*/10 * * * *"
    pm2 list
    or
    pm2 start app.yml
    ------------- app.yml example---        
      - script: /home/ubuntu/goparrot/app.py
      args: "--mode all --path data/ --start 100_years_ago"
      name: "app"
      cron: "0/5 * * * *"
      max-memory-restart: 500M
      watch : true
      interpreter: python
      cwd: /home/ubuntu/goparrot/
      
# Optional arguments:

  -h, --help          
    
    show this help message and exit
  
  -d DEST, --dest DEST
    
    mongodb URI to mongo instance, like   mongodb://db_name:password@ip:port/admin
```bash
        python app.py --dest mongodb://127.0.0.1:57017/admin
```
  -db DB_NAME
                                
    name of the database on mongo instance specified in --dest
    default: `go_parrot`
  
  -m {all,simulate},    --mode {all,simulate}
  
    import all files or simulate
    defaul: 'all`
                        
  -s START, --start START
  
        start date for import period: `1 day ago`, `1_january_2020`
        `1 hours ago`, `1_year_and_1_month_ago`, ...
        default = `100_years_ago`
```python
     python app.py --start 2_years_ago
```
                
  -e END, --end END   
  
        end of period: 1_day_ago`, `1_january_2020`, `1 year ago`...
        default = `1_day_in`
        ex: python app.py --start 2_years_ago  --end tomorrow        
        see [dateparser relative dates ](https://dateparser.readthedocs.io/en/latest/#relative-dates ) for details
        see [dateparser relative dates ]: https://dateparser.readthedocs.io/en/latest/#relative-dates  for details

  -f FREQ, --freq FREQ 
   
        cron freq for simulating ex: 5min 12h 1M

    Available freqs
    
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


# Diagrams    
![alt text](https://github.com/tumikosha/go_parrot/blob/master/DOCS/html/images/import_process.png)
    
![alt text](https://github.com/tumikosha/go_parrot/blob/master/DOCS/html/images/Entity_Relationship_Diagram1.png?raw=true)

# Requirements
Anaconda / Python 3.7.4

Mongodb 4  on port 57017 is required and started in docker container in `run.sh`

App creates database `go_parrot`

    `go_parrot.order` - table/collection with orders
    
    `go_parrot.customers`  - table/collection with users
    
    go_parrot.customers['is_empty'] = True if order refers to user without description
    
tables linked by fields: go_parrot.order['user_id'] ----> go_parrot.customers['_id']


cron service scans `data/` every 5min and try to import only records with

    `updated_at` > max(updated_at) - TIME_DELTA
    
    where  TIME_DELTA- configurable parameter (0 sec by default)
    
            max(updated_at) - calculated from db

In event based systems, the order of events may be disturbed in a small range (1 min?)

TIME_DELTA parameter solves this problem

in code it looks like:
```
    mask = (df['updated_at'] > (start_moment - config.TIME_DELTA)) & (df['updated_at'] <= end_moment)
    batch = df.loc[mask] # records filtered to window 
```

 see TIME_DELTA in config.py
 ```diff
+ I recommend to use TIME_DELTA = "24 hours ago" to be extra safe
    or you can just import all files one time a day
``` 

 
----------------------------------------------------------------
