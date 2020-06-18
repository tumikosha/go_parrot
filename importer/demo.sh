echo "demo.sh................"
python app.py --mode all --dest mongodb://127.0.0.1:57017/admin
# pause "Press [Enter] to run simulation"
python app.py --mode simulate --freq 12h  --dest mongodb://127.0.0.1:57017/admin