WITH ticket_flights_info AS (
    SELECT
        flight_id,
        COUNT(ticket_no) as psngr_num
    FROM
        ticket_flights
    GROUP BY
        flight_id
)

CREATE OR REPLACE VIEW airport_traffic AS

SELECT
    a.airport_code,
    -- суммируем количество попавших в аэропорт прилетающих рейсов
    SUM(CASE
        WHEN fl.departure_airport = a.airport_code
        THEN 1
        ELSE 0) AS departure_flights_num,
    -- суммируем заранее агрегированные данные по количеству людей в каждом вылетающем рейсе
    SUM(CASE
        WHEN fl.departure_airport = a.airport_code
        THEN tf.psngr_num
        ELSE 0) AS departure_psngr_num,
    -- суммируем количество попавших в аэропорт вылетающих рейсов
    SUM(CASE
        WHEN fl.arrival_airport = a.airport_code
        THEN 1
        ELSE 0) AS arrival_flights_num,
    -- суммируем заранее агрегированные данные по количеству людей в каждом прилетающем рейсе
    SUM(CASE
        WHEN fl.departure_airport = a.airport_code
        THEN tf.psngr_num
        ELSE 0) AS arrival_psngr_num,

FROM
    airports a
    LEFT JOIN flights fl
        ON a.airport_code = fl.departure_airport OR a.airport_code = fl.arrival_airport
    LEFT JOIN ticket_flights_info tf
        ON tf.flight_id = fl.flight_id

GROUP BY
    a.airport_code;