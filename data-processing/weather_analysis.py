from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("weather_processing").getOrCreate()


# Run after this point in pyspark shell.
target_jdbc = "jdbc:postgresql://database-1.c4ltkbfpq89z.us-east-1.rds.amazonaws.com:5432/postgres"
target_properties = {"user": "postgres", "password": "hotchocolate", "driver": "org.postgresql.Driver"}

weather_data_raw = spark.read.option("header", "true").csv("s3://airspace-weather-data/weather_data_gz/")

weather_data = weather_data_raw.filter("date >= '2016-01-01'")  # Take last 5 yrs data.

precipitation = weather_data.filter("element = 'PRCP'")\
    .select("id", "date", "value").withColumnRenamed("value", "precipitation")

wind_speed = weather_data.filter("element = 'AWND'")\
    .select("id", "date", "value").withColumnRenamed("value", "average_wind_speed")

tmax = weather_data.filter("element = 'TMAX'")\
    .select("id", "date", "value").withColumnRenamed("value", "max_temperature")

tmin = weather_data.filter("element = 'TMIN'")\
    .select("id", "date", "value").withColumnRenamed("value", "min_temperature")

precipitation.write.jdbc(target_jdbc, "precipitation", properties=target_properties)
wind_speed.write.jdbc(target_jdbc, "wind_speed", properties=target_properties)
tmax.write.jdbc(target_jdbc, "tmax", properties=target_properties)
tmin.write.jdbc(target_jdbc, "tmin", properties=target_properties)

weather_stations = spark.read.option("header", "true").csv("s3://airspace-weather-data/metadata/")
weather_stations.write.jdbc(target_jdbc, "weather_stations", properties=target_properties)


#The below join should happen in Flask
joined = weather_data.join(weather_stations, on=["id"]) \
    .select("latitude", "longitude", "date", "element", "value")

joined.show()

precipitation = joined.filter("element = 'PRCP'").withColumnRenamed("value", "precipitation")

precipitation.show()
