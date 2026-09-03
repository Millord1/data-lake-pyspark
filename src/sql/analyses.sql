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
    AVG(bikes) AS avg_bikes
FROM velib
GROUP BY station_id
ORDER BY avg_bikes DESC;

-- name: average_bike
SELECT
    to_timestamp(ts_utc) AS year,
    MONTH(to_timestamp(ts_utc)) AS month,
    ROUND(AVG(bikes), 2) AS avg_bikes,
    ROUND(AVG(capacity), 2) AS avg_capacity
FROM velib
GROUP BY year, month
ORDER BY year, month;

-- name: filling
SELECT
    YEAR(to_timestamp(ts_utc)) AS year,
    MONTH(to_timestamp(ts_utc)) AS month,
    ROUND(
        100.0 * SUM(bikes) / NULLIF(SUM(capacity), 0),
        2
    ) AS occupancy_rate
FROM velib
GROUP BY year, month
ORDER BY year, month;

-- name: most_used_stations
SELECT
    station_id,
    name,
    ROUND(AVG(bikes), 2) AS avg_bikes,
    MAX(bikes) AS max_bikes,
    MAX(capacity) AS capacity
FROM velib
GROUP BY station_id, name
ORDER BY avg_bikes DESC
LIMIT 20;

-- name: most_empty_stations
SELECT
    station_id,
    name,
    ROUND(AVG(bikes), 2) AS avg_bikes,
    MAX(capacity) AS capacity
FROM velib
GROUP BY station_id, name
ORDER BY avg_bikes
LIMIT 20;

-- name: almost_empty_stations
SELECT
    station_id,
    name,
    COUNT(*) AS observations,
    SUM(CASE WHEN bikes <= 1 THEN 1 ELSE 0 END) AS nearly_empty,
    ROUND(
        100.0 * SUM(CASE WHEN bikes <= 1 THEN 1 ELSE 0 END) / COUNT(*),
        2
    ) AS pct_nearly_empty
FROM velib
GROUP BY station_id, name
HAVING COUNT(*) > 100
ORDER BY pct_nearly_empty DESC
LIMIT 20;


-- name: frequently_full_stations
SELECT
    station_id,
    name,
    COUNT(*) AS observations,
    SUM(
        CASE
            WHEN bikes >= capacity - 1 THEN 1
            ELSE 0
        END
    ) AS nearly_full,
    ROUND(
        100.0 * SUM(
            CASE
                WHEN bikes >= capacity - 1 THEN 1
                ELSE 0
            END
        ) / COUNT(*),
        2
    ) AS pct_nearly_full
FROM velib
GROUP BY station_id, name
HAVING COUNT(*) > 100
ORDER BY pct_nearly_full DESC
LIMIT 20;

-- name: daily_usage
SELECT
    HOUR(to_timestamp(ts_utc)) AS hour,
    ROUND(AVG(bikes), 2) AS avg_bikes,
    ROUND(AVG(capacity), 2) AS avg_capacity,
    ROUND(
        100.0 * AVG(bikes) / NULLIF(AVG(capacity), 0),
        2
    ) AS occupancy_rate
FROM velib
GROUP BY hour
ORDER BY hour;