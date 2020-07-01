This script execute scenarios described in *.YAML file


# Run scenario described in file `config.yaml`
```
    python app.py --yaml config.yaml
    python app.py --yaml config.yaml --start 1_year_and_2_months_ago_00:00  --end 2_days_in
```
	
# Simulate 5min activity:
	python app.py --mode simulate --freq 5min --yaml simulate.yaml --start 100_years_and_2_months_ago_00:00  --end 2_days_in		
	
# Run service by cron:
    cd importer
	flask crontab add
	
# Run service by PM2:	
[[pm2.keymetrics.io](https://pm2.keymetrics.io) ]
However, instead of cron, I recommend using the pm2 utility in combination with relative dates and avoid the flask-cron.
This will keep control of the command line and memory and cron too
It will allow you to forget about cron and make the task much easier.
PM2 is a daemon process manager that will help you manage and keep your application online 24/7
   
     
    
    pm2 start 'python app.py --yaml config.yaml ' --name app --cron "*/10 * * * *"
    pm2 list
    or
    pm2 start app.yml
    ------------- app.yml example---        
      - script: /home/ubuntu/goparrot/app.py
      args: "--yaml config.yaml --start 100_years_ago"
      name: "app"
      cron: "0/5 * * * *"
      max-memory-restart: 500M
      watch : true
      interpreter: python
      cwd: /home/ubuntu/goparrot/
      
# Optional arguments:

  -h, --help          
    
    show this help message and exit
  
  -y FILE, --yaml FILE
    path *.yaml file
    
```bash
        python app.py --yaml step_1.yaml
```
  
  -m {run,simulate},    --mode {run,simulate}
  
    run scenario file or simulate
    defaul: 'run`
                        
  -s START, --start START
  
        start date for import period: `1 day ago`, `1_january_2020`
        `1 hours ago`, `1_year_and_1_month_ago_00:00`, ...
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

# Scenarios described in YAML files

Script makes following steps:   

    Step1. Loads orders and users from different sources(aka points) 
    Step2. Filter & grouping 
    Step3. Join  Orders and Users
    Step4. write results to destination points

 Yaml file consists of source and destination points of different types:
 [csv_orders, csv_users, orders_mongo_source, users_mongo_source, csv_dest, mongo_dest]
 
 Source points:

    type: csv_orders - the source of csv file with orders
    type: csv_users - the source of csv file with users
    type: orders_mongo_source - mongo db collection with orders
    type: users_mongo_source - mongo db collection with ousers
    
Destination points:    
    
    type: mongo_dest  - output mongo db with collections 
    type: csv_dest - file with full_orders (joined order and user)   
    
    # Type of iterators, use it to iterate over sources


    # Allowed Point types
    class PointType(Enum):
        CSV_ORDERS = "csv_orders" # CSV ORDER FILE
        CSV_USERS = "csv_users" # CSV USER FILE
        ORDERS_MONGO_SOURCE = 'orders_mongo_source' # MONGODB with  ORDERs
        USERS_MONGO_SOURCE = 'users_mongo_source' # MONGODB with  USERS
        MONGO_DEST = 'mongo_dest'  # MONGODB for full_orders and errors
        CSV_DEST = 'CSV_dest'  # CSV with  full_orders

Example:    

    points:
      source_orders_1:       # just the name of point, redefine it as you wish
        type: csv_orders  
        uri: data/orders_xxx.csv        # path to file
        dtype:             # field formats as  pandas.dataFrame DTYPE
          user_id: S       # means this this field is string
      source_u1:
        type: csv_users
        uri: data/users_xxx.csv
        dtype:
          user_id: S
          phone_number: S
          created_at: S
          updated_at: S

   
   See `config.yaml` for detailed example


# Error management
Need to redefine following methods in ETL.py
```
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
```
incorrect records goes to tables described in YAML file 
    error_users: error_users 
    error_orders: error_orders
    error_full_orders: error_full_orders


# Full example of destination point
```
  destination_final:
    type: mongo_dest
    uri: 'mongodb://127.0.0.1:57017/admin'
    db_name: final
    table_name: full_orders  # default: full_orders
    filtered_users_table_name: 'users' # default: None
    filtered_orders_table_name: 'orders' # default: None
    error_users: error_users
    error_orders: error_orders
    error_full_orders: error_full_orders
    unfiltered_orders_table_name: unfiltered_orders # default: None
    unfiltered_users_table_name: unfiltered_users # default: None
    erase_point_on_start: True # remove all records on start # default: False
```


# Requirements
Anaconda / Python 3.7.4

Mongodb 4  on port 57017 is required and started in docker container in `run.sh`


 
----------------------------------------------------------------
