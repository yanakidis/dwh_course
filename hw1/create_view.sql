CREATE OR REPLACE VIEW airport_traffic AS

SELECT
    a.airport_code,
    COUNT(DISTINCT f1.flight_id) AS departure_flights_num,
    COUNT(DISTINCT tf1.ticket_no) AS departure_psngr_num,
    COUNT(DISTINCT f2.flight_id) AS arrival_flights_num,
    COUNT(DISTINCT tf2.ticket_no) AS arrival_psngr_num

FROM
    airports a
    LEFT JOIN flights f1
        ON a.airport_code = f1.departure_airport
    LEFT JOIN ticket_flights tf1
        ON f1.flight_id = tf1.flight_id
    LEFT JOIN flights f2
        ON a.airport_code = f2.arrival_airport
    LEFT JOIN ticket_flights tf2
        ON f2.flight_id = tf2.flight_id

GROUP BY a.airport_code;