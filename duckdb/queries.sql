-- DUCKDB OLAP QUERIES THAT WILL FEED YOUR DASHBOARD
-- The dashboard itself cannot be powered from the pipeline.
-- The pipeline just creates the data to use with it.

CREATE TABLE IF NOT EXISTS rides AS 
    SELECT * FROM read_parquet('$LOADPATH');

ALTER TABLE rides ALTER temperature TYPE INTEGER;
ALTER TABLE rides ALTER humidity TYPE INTEGER;

-- Ride pickups by pickup date
CREATE OR REPLACE VIEW measures_by_time AS (
	SELECT 
	  EXTRACT(year FROM pickup_datetime) as year,
	  EXTRACT(month FROM pickup_datetime) as month, 
	  EXTRACT(day FROM pickup_datetime) as day,
	  EXTRACT(hour FROM pickup_datetime) as hour,
	  EXTRACT(minute FROM pickup_datetime) as minute,
	  COUNT(*) as total_rides,
	  AVG(passenger_count) as avg_passengers,
	  AVG(trip_distance) as avg_distance,
	  AVG(fare_amount) as avg_fare,
	  AVG(total_amount) as avg_total_amount,
	  SUM(fare_amount) as sum_fare,
	  SUM(total_amount) as total_revenue
	FROM rides
	GROUP BY ROLLUP (
	  year, month, day, hour, minute
	)
);

-- Ride pickups by day of week and hour
CREATE OR REPLACE VIEW measures_by_dow AS (
	SELECT
	  DAYNAME(pickup_datetime) AS dow,
	  EXTRACT(hour FROM pickup_datetime) AS hour,
	  COUNT(*) as total_rides,
	  AVG(passenger_count) as avg_passengers,
	  AVG(trip_distance) as avg_distance,
	  AVG(fare_amount) as avg_fare,
	  AVG(total_amount) as avg_total_amount,
	  SUM(fare_amount) as sum_fare,
	  SUM(total_amount) as total_revenue
	FROM rides
	GROUP BY GROUPING SETS ((dow, hour), (dow), (hour))
	ORDER BY dow, hour
);

-- Measures by weather condition
CREATE OR REPLACE VIEW measures_by_weather AS (
	SELECT
	  DAYNAME(pickup_datetime) AS dow,
	  EXTRACT(hour FROM pickup_datetime) AS hour,
	  condition,
          severity,
	  COUNT(*) as total_rides,
	  AVG(passenger_count) as avg_passengers,
	  AVG(trip_distance) as avg_distance,
	  AVG(fare_amount) as avg_fare,
	  AVG(total_amount) as avg_total_amount,
	  SUM(fare_amount) as sum_fare,
	  SUM(total_amount) as total_revenue
	FROM rides
        WHERE dow IS NOT NULL AND
		hour IS NOT NULL AND
		dow IS NOT NULL AND
		condition IS NOT NULL AND
		severity IS NOT NULL
	GROUP BY GROUPING SETS ((condition, severity), (condition, dow), 
		(condition, severity, dow), (condition), (condition, severity, dow, hour),
		(condition, dow, hour))
	ORDER BY dow, hour, condition, severity
);



