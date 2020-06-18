clear
pip install -r requirements.txt
docker stop mongodb
docker stop goparrot
docker rm mongodb
docker pull mongo:4.0.4
docker run --detach --name mongodb -p 57017:27017  mongo:4.0.4
echo "==============run crontab============================"
flask crontab add
echo "================================================="
echo "import all files from dir data/"
python app.py --mode all --dest mongodb://127.0.0.1:57017/admin
#echo "simulate 5min activity"

#python app.py --mode simulate -freq 5min  --dest mongodb://127.0.0.1:57017/admin
# docker exec -it mongodb bash
# docker exec -it goparrot bash