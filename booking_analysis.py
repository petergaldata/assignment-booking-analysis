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


if __name__ == "__main__":
    bookings_path = "data/bookings/booking.json"
    bookings = read_and_validate_bookings(bookings_path)
    bookings.createOrReplaceTempView("bookings")

    airports_path = "data/airports/airports.dat"
    airports = read_airports(airports_path)
    airports.createOrReplaceTempView("airports")

    result = spark.sql(
        """
        WITH confirmed_date_localized_bookings as(
            SELECT 
            b.*, 
            from_utc_timestamp(b.departure_date, a.tz) AS departure_date_local
            FROM bookings b
            INNER JOIN (
                SELECT passenger_uci, MAX(timestamp) as max_timestamp
                FROM bookings
                GROUP BY passenger_uci
            ) b_max ON b.passenger_uci = b_max.passenger_uci AND b.timestamp = b_max.max_timestamp
            INNER JOIN airports a on b.origin_airport = a.iata
            WHERE b.booking_status = 'CONFIRMED' AND b.operating_airline = 'KL'
        )
        SELECT 
        ap_dest.country AS destination_country,
        DATE_FORMAT(to_timestamp(cb.departure_date_local), 'E') AS day_of_week, 
        CASE 
            WHEN MONTH(to_timestamp(cb.departure_date_local)) IN (12, 1, 2) THEN 'Winter'
            WHEN MONTH(to_timestamp(cb.departure_date_local)) IN (3, 4, 5) THEN 'Spring'
            WHEN MONTH(to_timestamp(cb.departure_date_local)) IN (6, 7, 8) THEN 'Summer'
            ELSE 'Autumn'
            END AS season,
        COUNT(DISTINCT cb.passenger_uci) AS number_of_passengers
        FROM confirmed_date_localized_bookings cb
        INNER JOIN airports ap_orig ON cb.origin_airport = ap_orig.iata
        INNER JOIN airports ap_dest ON cb.destination_airport = ap_dest.iata
        WHERE
            ap_orig.country = 'Netherlands'
        GROUP BY 
            destination_country, day_of_week, season
        ORDER BY 
            number_of_passengers DESC, season, day_of_week;
        """ 
    )
    result.show()

    """
    TODO:
    ---most popular destination in network---
    -DONE which countries are the most popular per season
    -DONE for each day of the week, over a given time period
    -DONE only KL flights departing from the Netherlands.
    -DONE day of the week should be based on the local timezone for the airport.
    -DONE passenger should only be counted once per flight leg.
    -DONE We should only count a passenger if their latest booking status is Confirmed.

    -DONE The output should be a table where each row shows :
        the number of passengers per country, per day of the week, per season. The table should be sorted in descending order by the number of bookings, grouped by season and day of the week.
    """

    """
    functional req:
    user should be able to specify a local directory as input for the bookings
    The user should be able to specify a start-date and end-date
    your solution should be able to take as input a directory location on HDFS, containing many files in the same format totaling TBs in size.
    add docker compose to repo
    """