# go_parrot
This script imports all files with orders_xxxx.csv/users_xxxx.csv from directory `data/`

import all files:

	python app.py --mode all -path data/ --dest mongodb://127.0.0.1:57017/admin
	
simulate 5min activity:

	python app.py --simulate -freq 5min all --dest mongodb://127.0.0.1:57017/admin
	
run service by cron:

	flask crontab add

NOTE: Simulatiom mode erases DB before run

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
    
    where  TIME_DELTA- configurable parameter (1 sec by default)
    
            max(updated_at) - calculated from db


# command line params
optional arguments:

  -h, --help            show this help message and exit
  
  -d DEST, --dest DEST  mongodb URI like
  
                        mongodb://db_name:password@ip:port/admin
			
  -p PATH, --path PATH  path to dir with files
  
  -m {all,simulate}, --mode {all,simulate}
  
                        import all files or simulate
			
  -f FREQ, --freq FREQ  cron freq for simulating ex: 5min 12h 1M
  
© 2020 GitHub, Inc.
