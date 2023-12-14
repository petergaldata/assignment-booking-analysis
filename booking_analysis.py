from pyspark.sql import SparkSession, types
from pyspark.sql.functions import col, explode


spark = SparkSession.builder \
        .appName("Spark SQL Query") \
        .getOrCreate()


def read_and_validate_bookings(json_path):
    bookings = spark.read.option("mode", "PERMISSIVE").json(json_path)
    if "_corrupt_record" in bookings.columns:
        bookings = bookings.filter(col("_corrupt_record").isNull())
        bookings_corrutpted = bookings.filter(col("_corrupt_record").isNotNull())
    bookings = bookings.withColumn("passenger", explode("event.DataElement.travelrecord.passengersList")).withColumn("product", explode("event.DataElement.travelrecord.productsList"))
    bookings = bookings.select(
        col("timestamp").alias("timestamp"),
        col("passenger.uci").alias("passenger_uci"),
        col("passenger.age").alias("passenger_age"),
        col("passenger.passengerType").alias("passenger_type"),
        col("product.bookingStatus").alias("booking_status"),
        col("product.flight.operatingAirline").alias("operating_airline"),
        col("product.flight.originAirport").alias("origin_airport"),
        col("product.flight.destinationAirport").alias("destination_airport"),
        col("product.flight.departureDate").alias("departure_date"),
        col("product.flight.arrivalDate").alias("arrival_date")
    )
    return bookings
    

def read_airports(data_path):
    column_names = ["airport_id", "name", "city", "country", "iata", "icao", "latitude", "longitude", "altitude", "timezone", "dst", "tz", "type",	"source"]
    airports = spark.read.option("delimiter", ",").csv(data_path)
    airports = airports.toDF(*column_names)
    return airports

def execute_sql_analysis(analysis_sql_path):
    with open(analysis_sql_path, "r") as file:
        sql_query = file.read()
    result = spark.sql(sql_query)
    return result

if __name__ == "__main__":
    bookings_path = "data/bookings/booking.json"
    bookings = read_and_validate_bookings(bookings_path)
    bookings.createOrReplaceTempView("bookings")

    airports_path = "data/airports/airports.dat"
    airports = read_airports(airports_path)
    airports.createOrReplaceTempView("airports")
    analysis_sql_path = "booking_analysis.sql"

    result = execute_sql_analysis(analysis_sql_path)
    result.show()

    """
    functional req:
    user should be able to specify a local directory as input for the bookings
    The user should be able to specify a start-date and end-date
    your solution should be able to take as input a directory location on HDFS, containing many files in the same format totaling TBs in size.
    add docker compose to repo
    """