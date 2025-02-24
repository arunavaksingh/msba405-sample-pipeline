# Sample MSBA 405 Pipeline (Final Project)
# Pipeline to Power NYC Taxi vs Weather Analysis

**NOTE: To run certain examples, see the README in the luigi and airflow directories.**

In the `README.md`you must provide enough information for us to download the dataset
and run the pipeline on our own machines.

This should be "fire and forget." We should be able to execute one command to run the pipeline.

1. The Data

For this analysis we used 

We went to this page: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
and downloaded the Yellow Taxi data for October 2024 in Parquet format.

The link to the data is:

wget https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-11.parquet

We also downloaded the zone mapping for pickup and dropoff locations from:

wget https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv

It is available on the same website on Taxi Zone Lookup Table.

We then downloaded hourly weather conditions for New York City for NOAA.

wget https://www.ncei.noaa.gov/data/local-climatological-data/access/2024/

The proper weather station ID is 72505394728. The direct link is:

https://www.ncei.noaa.gov/data/local-climatological-data/access/2024/72505394728.csv

Clone this repo containing our code and move the data files into the data directory of the repo
so the pipeline can read them.

Then run

bash pipeline.sh

TIP FOR STUDENTS: Put your data into the root of your git repo so it's easy to run your
code BUT add the file extensions to .gitignore so that the data is not committed to Github.
The data for this sample pipeline has extension .parquet and .csv. See the .gitignore file
for an example!
