from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_timestamp, date_trunc, row_number, split,
    regexp_extract, regexp_replace, when, 
    monotonically_increasing_id, broadcast
)
from pyspark.sql import functions as F
from pyspark.sql import Window

# This should prevent your job from using too much RAM and crashing your
# EC2 instance.
# This is not required unless running in interactive mode.
# spark = SparkSession.builder \
#    .master("local[*]") \
#    .config("spark.driver.memory", "250m") \
#    .config("spark.executor.memory", "250m") \
#    .config("spark.sql.shuffle.partitions", "2") \
#    .getOrCreate()


def main():
    spark = SparkSession.builder.getOrCreate()
    # Load and preprocess rides data
    # 1, We only retain certain columns and rename several of them.
    # 2. Some of the dates are invalid (2026), so we restrict to 2024.
    # 3. We also created a sequence ID as a primary key to make sure we have
    #    the correct number of rows.
    # 4. We only care about aggregate rides at the hour level, so we converted
    #    the timestamp to date and hour: 2024-10-01 08:00:00 for 8am hour
    rides = (
        spark.read
        .option("header", "true")
        .option("recursiveFileLookup", "true")
        .option("spark.sql.files.maxPartitionBytes", "128MB")  # should save RAM
        .parquet("yellow_tripdata_2024-10.parquet")
        .select(
            col('VendorID'),
            col('tpep_pickup_datetime').alias('pickup_datetime'),
            col('tpep_dropoff_datetime').alias('dropoff_datetime'),
            col('passenger_count'),
            col('trip_distance'),
            col('PULocationID').alias('pickup_location'),
            col('DOLocationID').alias('dropoff_location'),
            col('fare_amount'),
            col('total_amount')
        )
        .filter((F.year(col('pickup_datetime')) == 2024) & (F.year(col('dropoff_datetime')) == 2024))
        .withColumn("seq", monotonically_increasing_id())
        .withColumn('pickup_hour', date_trunc('hour', 'pickup_datetime'))
    )

    # We may need some geographical information for our dashboard.
    # Let's load the taxi zone lookup table.
    zones = spark.read.csv("taxi_zone_lookup.csv", header=True)

    # We then joined rides with zone so we know where in NYC each trip started and ended.
    # The join gaec us some ugly names so we renamed those columns.
    rides = (
        rides
        .join(broadcast(zones).alias('pickup'), rides.pickup_location == col('pickup.LocationID'))
        .join(broadcast(zones).alias('dropoff'), rides.dropoff_location == col('dropoff.LocationID'))
        .select(
            'seq', 'VendorID', 'pickup_datetime', 'dropoff_datetime', 'passenger_count', 
            'trip_distance', 'pickup_location', 'dropoff_location', 'fare_amount', 
            'total_amount', 'pickup_hour',
            col('pickup.Borough').alias('pickup_borough'),
            col('pickup.Zone').alias('pickup_zone'),
            col('dropoff.Borough').alias('dropoff_borough'),
            col('dropoff.Zone').alias('dropoff_zone')
        )
    )

    # Load and preprocess weather data for the NYC Central Park weather station.
    # 1. We first converted each observation to an hour: 2024-10-01 08:00:00
    # 2. The data is for the entire year, so we restricted to October 2024.
    # 3. We only need certain columns.
    # 4. We did some special processing on the weather type field which sometimes
    #    contained multiple conditions if it rained lightly, then rained, then rained
    #    severely. We decided to only keep the first condition for each hour.
    # 5. We stored the severity in another column using Spark's equivalent of a CASE
    #    statement.
    # 6. In cases where precipitation was Trace, we placed the T with 0. We don't care.
    # 7. Some precipitation amounts had an "s" after them. We removed them.
    # 8. We noticed that our joins were blowing up and requiring way too much RAM.
    #    Some hours had multiple observations and we have a 1:M join instead of 1:1
    #    so we used a window function to restrict to the first observation in the
    #    hour.
    # 9. Finally we only projected certain columns and renamed some.
    weather = (
       # Read data
       spark.read.csv("72505394728.csv", header=True)
       
       # Convert DATE string to timestamp and get hour
       .withColumn('HOUR', date_trunc('hour', to_timestamp('DATE')))

       .filter(
            (F.year('HOUR') == 2024) &    # Using 2023 since 2024 October data doesn't exist yet
            (F.month('HOUR') == 10)
        )
       
       # Select base columns
       .select(
           'HOUR',
           'HourlyPrecipitation',
           'HourlyRelativeHumidity',
           'HourlyDryBulbTemperature',
           'HourlyPresentWeatherType'
       )
       
       # Process weather type
       .withColumn('weather_code_first', split('HourlyPresentWeatherType', r'\|')[0])
       .withColumn(
           'severity',
           when(col('weather_code_first').isNull(), None)
           .when(col('weather_code_first').contains('-'), 'light')
           .when(col('weather_code_first').contains('+'), 'heavy')
           .otherwise('moderate')
       )
       .withColumn(
           'condition',
           regexp_extract('weather_code_first', r'[+-]?(\w+)(?=:|$)', 1)
       )
       
       # Clean precipitation data
       .withColumn(
           'precipitation',
           when(col('HourlyPrecipitation') == 'T', '0')
           .otherwise(regexp_replace('HourlyPrecipitation', 's$', ''))
           .cast('float')
       )
       
       # Take first reading per hour
       .withColumn(
           'row_num', 
           row_number().over(Window.partitionBy('HOUR').orderBy('HOUR'))
       )
       .filter('row_num = 1')
       
       # Final columns
       .select(
           'HOUR',
           'precipitation',
           col('HourlyRelativeHumidity').cast('float').alias('humidity'),
           col('HourlyDryBulbTemperature').cast('float').alias('temperature'),
           'severity',
           'condition'
       )
    )

    # Join rides data with weather data
    # 1. Now that we eliminated duplicates in weather, we can join!
    # 2. We only kept certain columns.
    final = (
        rides.join(weather, rides.pickup_hour == weather.HOUR)
        .select(
            "seq", "VendorID", "pickup_datetime", "dropoff_datetime", "passenger_count", 
            "trip_distance", "fare_amount", "total_amount", "pickup_borough",
            "pickup_zone", "dropoff_borough", "dropoff_zone", "pickup_hour", 
            "precipitation", "humidity", "temperature", 
            "severity", "condition"
        )
    )

    # Write output to parquet, 100000 files per file.
    final.write \
       .option("maxRecordsPerFile", 100000) \
       .mode("overwrite") \
       .parquet("output")

if __name__ == "__main__":
    main()
