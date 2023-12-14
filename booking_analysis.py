import json
from datetime import date
import datetime
from pyspark.sql import SparkSession, types
from pyspark.sql.functions import col, explode


spark = SparkSession.builder \
        .appName("Spark SQL Query") \
        .getOrCreate()

def read_json_and_get_variables(file_path):

    try:
        with open(file_path, 'r') as file:
            data = json.load(file)

        bookings_location = data.get("bookings_location")
        airports_path = data.get("airports_location")

        min_date = date(datetime.MINYEAR, 1, 1)
        max_date = date(datetime.MAXYEAR, 12, 31)
        start_date = data.get("start_date", min_date)
        end_date = data.get("end_date", max_date)
        start_date = start_date if start_date != "" else min_date
        end_date = end_date if end_date != "" else max_date

        return bookings_location, airports_path, start_date, end_date

    except json.JSONDecodeError:
        print("Error: The file does not contain valid JSON.")
    except FileNotFoundError:
        print(f"Error: The file {file_path} was not found.")
    except KeyError as e:
        print(f"Error: Missing key {e} in JSON data.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

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

def execute_sql_analysis(analysis_sql_path, start_date, end_date):
    with open(analysis_sql_path, "r") as file:
        sql_query = file.read()
    sql_query = sql_query.replace("${start_date}", f"'{start_date}'")
    sql_query = sql_query.replace("${end_date}", f"'{end_date}'")
    result = spark.sql(sql_query)
    return result

if __name__ == "__main__":
    file_path = 'config.json'
    bookings_path, airports_path, start_date, end_date = read_json_and_get_variables(file_path)

    bookings = read_and_validate_bookings(bookings_path)
    bookings.createOrReplaceTempView("bookings")

    airports = read_airports(airports_path)
    airports.createOrReplaceTempView("airports")

    analysis_sql_path = "booking_analysis.sql"
    result = execute_sql_analysis(analysis_sql_path, start_date, end_date)
    result.show()

    """
    functional req:
    The user should be able to specify a start-date and end-date
    your solution should be able to take as input a directory location on HDFS, containing many files in the same format totaling TBs in size.
    add docker compose to repo
    """