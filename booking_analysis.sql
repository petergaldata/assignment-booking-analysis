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
    AND TO_DATE(cb.timestamp) between ${start_date} and ${end_date}
GROUP BY 
    destination_country, day_of_week, season
ORDER BY 
    number_of_passengers DESC, season, day_of_week;