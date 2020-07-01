echo "demo.sh................"

echo "### ---=== pupulate *_xxx.csv to db:src ===----"
python app.py --mode run --yaml step_1.yaml


echo "### ---=== populate *.CSV to src_all ===---"
python app.py  --start 16_month_ago --yaml step_1_all.yaml

echo "### ---=== populate from db:src_all --> db:simulate ===---"
# python app.py --mode simulate --freq 5min  --start 12_months_ago --yaml simulate.yaml
python app.py --mode simulate --freq 1D  --start 12_months_ago --yaml simulate.yaml
# add crontab
# flask crontab add