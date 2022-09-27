#!/usr/bin/env python

"""BigQuery I/O PySpark example."""

from pyspark.sql import SparkSession


spark = SparkSession \
  .builder \
  .master('yarn') \
  .appName('spark-bigquery-demo') \
  .getOrCreate()

# Use the Cloud Storage bucket for temporary BigQuery export data used
# by the connector.
bucket = "lbg-poc-data/temporaryGcsBucket"
spark.conf.set('temporaryGcsBucket', bucket)

# Load data from BigQuery.
bikeshare_stations = spark.read.format('bigquery') \
  .option('table', 'bigquery-public-data.san_francisco.bikeshare_stations') \
  .load()
bikeshare_stations.createOrReplaceTempView('bikeshare_stations')

bikeshare_trips = spark.read.format('bigquery') \
  .option('table', 'bigquery-public-data.san_francisco.bikeshare_trips') \
  .load()
bikeshare_trips.createOrReplaceTempView('bikeshare_trips')


result_df=bikeshare_stations.join(bikeshare_trips,bikeshare_stations.station_id ==  bikeshare_trips.start_station_id,"inner")

result_df.printSchema()

# Saving the data to BigQuery
result_df.write.format('bigquery') \
  .mode("overwrite") \
  .option('table', 'lbg_demo.bikeshare_result_spark_processing') \
  .save()