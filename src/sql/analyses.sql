-- name: test_query
SELECT *
FROM velib
LIMIT 5

-- name: count_stations
SELECT COUNT(DISTINCT station_id) AS nb_stations
FROM velib;


-- name: top_stations
SELECT
    station_id,
    COUNT(*) AS nb_records
FROM velib
GROUP BY station_id
ORDER BY nb_records DESC
LIMIT 10;


-- name: available_bikes_by_station
SELECT
    station_id,
    AVG(bikes_available) AS avg_bikes
FROM velib
GROUP BY station_id
ORDER BY avg_bikes DESC;

-- average_bike
SELECT 
    station_id,
    contract_name,
    AVG(available_bikes) AS avg_bikes,
    MAX(available_bike_stands) AS max_stands
FROM velib_open
GROUP BY station_id, contract_name
ORDER BY avg_bikes DESC