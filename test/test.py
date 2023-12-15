import unittest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DoubleType
import datetime

class SparkSQLTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder \
            .appName("KLM Destinations Analysis Unit Test") \
            .master("local[2]") \
            .getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_sql_query(self):
        bookings_schema = StructType([
            StructField("passenger_uci", StringType()),
            StructField("timestamp", TimestampType()),
            StructField("departure_date", TimestampType()),
            StructField("booking_status", StringType()),
            StructField("operating_airline", StringType()),
            StructField("origin_airport", StringType()),
            StructField("destination_airport", StringType()),
            StructField("passenger_age", IntegerType())
        ])

        airports_schema = StructType([
            StructField("iata", StringType()),
            StructField("country", StringType()),
            StructField("tz", StringType())
        ])

        # Mock data for bookings
        bookings_data = [
            ("P123", datetime.datetime(2023, 1, 1, 10, 0), datetime.datetime(2023, 1, 2, 8, 0), "CONFIRMED", "KL", "AMS", "JFK", 30),
            ("P124", datetime.datetime(2023, 1, 1, 11, 0), datetime.datetime(2023, 1, 2, 9, 0), "CONFIRMED", "KL", "AMS", "LAX", 25),
            ("P125", datetime.datetime(2023, 2, 15, 12, 0), datetime.datetime(2023, 2, 16, 11, 0), "CONFIRMED", "KL", "AMS", "CDG", 45),
            ("P111", datetime.datetime(2023, 2, 15, 12, 0), datetime.datetime(2023, 2, 16, 11, 0), "CONFIRMED", "KL", "AMS", "CDG", 45),
            ("P112", datetime.datetime(2023, 2, 15, 12, 0), datetime.datetime(2023, 2, 16, 11, 0), "CONFIRMED", "KL", "AMS", "CDG", 45),
            ("P126", datetime.datetime(2023, 3, 10, 9, 0), datetime.datetime(2023, 3, 11, 8, 0), "CONFIRMED", "KL", "AMS", "DXB", 37),
            ("P127", datetime.datetime(2023, 4, 25, 7, 0), datetime.datetime(2023, 4, 26, 6, 0), "CONFIRMED", "KL", "AMS", "NRT", 50),
            ("P128", datetime.datetime(2023, 5, 20, 20, 0), datetime.datetime(2023, 5, 21, 19, 0), "CONFIRMED", "KL", "AMS", "SIN", 28),
            ("P128", datetime.datetime(2023, 5, 20, 20, 0), datetime.datetime(2023, 5, 21, 19, 0), "CONFIRMED", "AF", "AMS", "SIN", 28)
        ]

        # Mock data for airports
        airports_data = [
            ("AMS", "Netherlands", "Europe/Amsterdam"),
            ("JFK", "USA", "America/New_York"),
            ("LAX", "USA", "America/Los_Angeles"),
            ("CDG", "France", "Europe/Paris"),
            ("DXB", "UAE", "Asia/Dubai"),
            ("NRT", "Japan", "Asia/Tokyo"),
            ("SIN", "Singapore", "Asia/Singapore")
        ]

        bookings_df = self.spark.createDataFrame(bookings_data, bookings_schema)
        airports_df = self.spark.createDataFrame(airports_data, airports_schema)

        bookings_df.createOrReplaceTempView("bookings")
        airports_df.createOrReplaceTempView("airports")

        query = """
            WITH confirmed_date_localized_bookings as(
                SELECT 
                b.*, 
                from_utc_timestamp(b.departure_date, ap_orig.tz) AS departure_date_local
                FROM bookings b
                INNER JOIN (
                    SELECT passenger_uci, MAX(timestamp) as max_timestamp
                    FROM bookings
                    GROUP BY passenger_uci
                ) b_max ON b.passenger_uci = b_max.passenger_uci AND b.timestamp = b_max.max_timestamp
                INNER JOIN airports ap_orig on b.origin_airport = ap_orig.iata
                WHERE b.booking_status = 'CONFIRMED' AND b.operating_airline = 'KL' AND ap_orig.country = 'Netherlands'
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
                COUNT(DISTINCT cb.passenger_uci) AS number_of_passengers,
                COUNT(DISTINCT CASE WHEN cb.passenger_age >= 18 THEN cb.passenger_uci ELSE NULL END) AS number_of_adults,
                COUNT(DISTINCT CASE WHEN cb.passenger_age < 18 THEN cb.passenger_uci ELSE NULL END) AS number_of_children,
                ROUND(AVG(cb.passenger_age), 2) AS average_age
            FROM confirmed_date_localized_bookings cb
            INNER JOIN airports ap_dest ON cb.destination_airport = ap_dest.iata
            GROUP BY 
                destination_country, day_of_week, season
            ORDER BY 
                number_of_passengers DESC, season, day_of_week;
        """

        result_df = self.spark.sql(query)
        expected_schema = StructType([
            StructField("destination_country", StringType(), True),
            StructField("day_of_week", StringType(), True),
            StructField("season", StringType(), True),
            StructField("number_of_passengers", IntegerType(), True),
            StructField("number_of_adults", IntegerType(), True),
            StructField("number_of_children", IntegerType(), True),
            StructField("average_age", DoubleType(), True)
        ])
        expected_data = [
                ("France", "Thu", "Winter", 3, 3, 0, 45.0),
                ("USA", "Mon", "Winter", 2, 2, 0, 27.5),  
                ("UAE", "Sat", "Spring", 1, 1, 0, 37.0),
                ("Japan", "Wed", "Spring", 1, 1, 0, 50.0),
                ("Singapore", "Sun", "Spring", 1, 1, 0, 28.0)
            ]

        expected_df = self.spark.createDataFrame(expected_data, expected_schema)
        self.assertEqual(sorted(result_df.collect()), sorted(expected_df.collect()))

if __name__ == '__main__':
    unittest.main()
