-- A query to find out what the rush hours were for each station in the last month. 

-- The busiest station has the highest count of trips over the top 3 rush hours.

WITH 

hourly_counts AS (
  SELECT 
    DATE(start_time) AS trip_date,
    start_station_id,
    EXTRACT(HOUR FROM start_time) AS hour_of_day,
    COUNT(*) AS num_trips
  FROM 
    `bigquery-public-data.austin_bikeshare.bikeshare_trips`
  WHERE 
    EXTRACT(HOUR FROM start_time) BETWEEN 7 AND 19
    AND format_timestamp('%Y%m', DATE(start_time)) = 
       CAST((CAST(format_timestamp('%Y%m', DATE(CURRENT_DATE)) as INT) -1) as STRING) --the last month data
  GROUP BY 
    DATE(start_time),
    start_station_id,
    DATE(start_time),
    EXTRACT(HOUR FROM start_time)
),

rush_hours AS (
  SELECT 
    start_station_id,
    trip_date,
    hour_of_day,
    num_trips,
    ROW_NUMBER() OVER (
      PARTITION BY start_station_id
      ORDER BY num_trips DESC 
    ) AS row_num
  FROM 
    hourly_counts
)

SELECT 
  start_station_id,
  s.name,
  trip_date,
  hour_of_day,
  num_trips,
  FORMAT_DATE("%A", trip_date) AS day_of_week,
  SUM(num_trips) OVER(PARTITION BY start_station_id) total
FROM 
  rush_hours rh
  JOIN `bigquery-public-data.austin_bikeshare.bikeshare_stations` s
      ON rh.start_station_id = s.station_id
WHERE 
  row_num <= 3
ORDER BY 
total  DESC