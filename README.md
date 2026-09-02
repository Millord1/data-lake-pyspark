Run Docker compose for mongoDB container : 

# Start container
```
docker compose --env-file .env -f docker/docker-compose.yml up -d mongodb
```

# Delet containers & volumes 
```
docker compose --env-file .env -f docker/docker-compose.yml down -v
```

# Connection to Weather_landing and check 1st element 

```docker exec -it distant_doc_mongodb mongosh -u admin -p password123 --authenticationDatabase admin weather_landing --eval "
db.raw_weather_archive.find({}, {year: 1, start_date: 1, end_date: 1, ingested_at: 1, 'payload.elevation': 1}).toArray();
"
```