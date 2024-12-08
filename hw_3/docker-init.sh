docker-compose down

echo "Clearing data ---"
rm -rf data
rm -rf data-slave

docker-compose up -d  postgres

echo "Starting postgres_master node... ---"
sleep 60  # Waits for master note start complete

echo "Prepare replica config... ---"
docker exec -it postgres bash /etc/postgresql/init-script/init.sh
echo "Restart master node ---"
docker-compose restart postgres
sleep 30

echo "Starting slave node... ---"
docker-compose up -d  postgres_slave
sleep 30  # Waits for note start complete

echo "Done ---"

echo "Starting next... ---"
docker-compose up -d
sleep 30  # Waits for note start complete

echo "Done ---"
