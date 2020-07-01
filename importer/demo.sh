echo "demo.sh................"

echo "### ---=== pupulate *_xxx.csv to db:src ===----"
python app.py --mode all --yaml step_1.yaml
# pause "Press [Enter] to run simulation"
#python app.py --mode simulate --freq 12h  --dest mongodb://127.0.0.1:57017/admin

echo "### ---=== populate *.CSV to src_all ===---"
python app.py  --start 16_month_ago --yaml step_1_all.yaml

echo "### ---=== populate from db:src_all --> db:simulate ===---"
python app.py --mode simulate --freq 5min  --start 12_months_ago --yaml simulate.yaml

# add crontab
# flask crontab add