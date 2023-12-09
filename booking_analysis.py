from pyspark.sql import SparkSession

spark = SparkSession.builder \
        .appName("Spark SQL Query JSON data") \
        .getOrCreate()

bookings = spark.read.json("data/bookings/booking.json")
airports = spark.read.option("delimiter", ",").csv("data/airports/airports.dat")



bookings.show()
airports.show()