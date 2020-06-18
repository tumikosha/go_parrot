clear
docker stop mongodb
docker stop goparrot
docker rm mongodb
docker pull mongo:4.0.4
docker run --detach --name mongodb -p 57017:27017  mongo:4.0.4
docker stop goparrot
docker build .  -t goparrot -f Dockerfile
#docker run  --detach --network="host" -it --rm --name goparrot goparrot
docker run  --network="host" -it --rm --name goparrot goparrot
echo "---=== waiting few seconds while docker apps started"
sleep 15

