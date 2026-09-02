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