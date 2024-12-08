SELECT pg_sleep(2);

INSERT INTO bookings
(book_ref, book_date, total_amount)
VALUES
('ref_1', '2024-06-22 19:10:25-07', 3),
('ref_2', '2024-07-22 19:11:25-07', 2),
('ref_3', '2024-05-22 19:15:25-07', 1),
('ref_4', '2024-04-22 19:10:25-07', 1),
('ref_5', '2024-03-22 19:10:25-07', 2),
('ref_6', '2024-02-22 19:10:25-07', 2),
('ref_7', '2024-01-22 19:10:25-07', 4);

SELECT pg_sleep(2);

INSERT INTO tickets
(ticket_no, book_ref, passenger_id, passenger_name, contact_data)
VALUES
('ticket_1', 'ref_1', 'pas_1', 'Mike', NULL),
('ticket_2', 'ref_1', 'pas_2', 'Michael', NULL),
('ticket_3', 'ref_1', 'pas_3', 'George', NULL),
('ticket_4', 'ref_2', 'pas_4', 'Alex', NULL),
('ticket_5', 'ref_2', 'pas_5', 'Bob', NULL),
('ticket_6', 'ref_3', 'pas_2', 'Michael', NULL),
('ticket_7', 'ref_4', 'pas_6', 'Victor', NULL),
('ticket_8', 'ref_5', 'pas_7', 'Vlad', NULL),
('ticket_9', 'ref_5', 'pas_8', 'Denni', NULL),
('ticket_10', 'ref_6', 'pas_9', 'John', NULL),
('ticket_11', 'ref_6', 'pas_10', 'Jorah', NULL),
('ticket_12', 'ref_7', 'pas_1', 'Mike', NULL),
('ticket_13', 'ref_7', 'pas_2', 'Michael', NULL),
('ticket_14', 'ref_7', 'pas_5', 'Bob', NULL),
('ticket_15', 'ref_7', 'pas_7', 'Vlad', NULL);

SELECT pg_sleep(2);

INSERT INTO airports
(airport_code, airport_name, city, coordinates_lon, coordinates_lat, timezone)
VALUES
('MSK', 'Moscow', 'Moscow', 1, 23, '+3'),
('SPB', 'Saint Petersburg', 'Saint Petersburg', 12, 12, '+3'),
('IST', 'Istanbul', 'Istanbul', 2, 41, '+3'),
('KZN', 'Kazan', 'Kazan', 3, 123, '+4');

SELECT pg_sleep(2);

INSERT INTO aircrafts
(aircraft_code, model, range)
VALUES
('001', '{}', 1),
('002', '{}', 2),
('003', '{}', 3),
('004', '{}', 4),
('005', '{}', 5);

SELECT pg_sleep(2);

INSERT INTO flights
(flight_id, flight_no, scheduled_departure, scheduled_arrival, departure_airport, arrival_airport, status, aircraft_code, actual_departure, actual_arrival)
VALUES
(1, '1', '2024-06-22 19:10:25-07', '2024-06-22 19:10:25-07', 'MSK', 'SPB', 'NULL', '001', '2024-06-22 19:10:25-07', '2024-06-22 19:10:25-07'),
(2, '2', '2024-06-22 19:10:25-07', '2024-06-22 19:10:25-07', 'MSK', 'IST', 'NULL', '002', '2024-06-22 19:10:25-07', '2024-06-22 19:10:25-07'),
(3, '3', '2024-06-22 19:10:25-07', '2024-06-22 19:10:25-07', 'SPB', 'KZN', 'NULL', '003', '2024-06-22 19:10:25-07', '2024-06-22 19:10:25-07'),
(4, '4', '2024-06-22 19:10:25-07', '2024-06-22 19:10:25-07', 'KZN', 'IST', 'NULL', '004', '2024-06-22 19:10:25-07', '2024-06-22 19:10:25-07'),
(5, '5', '2024-06-22 19:10:25-07', '2024-06-22 19:10:25-07', 'IST', 'SPB', 'NULL', '005', '2024-06-22 19:10:25-07', '2024-06-22 19:10:25-07');

SELECT pg_sleep(2);

INSERT INTO ticket_flights 
(ticket_no, flight_id, fare_conditions, amount)
VALUES
('ticket_1', 1, 0, 100),
('ticket_2', 1, 0, 200),
('ticket_3', 1, 0, 1000),
('ticket_4', 2, 0, 300),
('ticket_5', 2, 0, 432),
('ticket_6', 2, 0, 625),
('ticket_7', 3, 0, 2345),
('ticket_8', 4, 0, 234),
('ticket_9', 4, 0, 134),
('ticket_10', 4, 0, 75),
('ticket_11', 4, 0, 134),
('ticket_12', 5, 0, 363),
('ticket_13', 5, 0, 534),
('ticket_14', 5, 0, 764),
('ticket_15', 5, 0, 632);

SELECT pg_sleep(2);

INSERT INTO seats 
(aircraft_code, seat_no, fare_conditions)
VALUES 
('001', '1', 'cond1'),
('002', '1', 'cond2');

SELECT pg_sleep(2);

INSERT INTO boarding_passes 
(ticket_no, flight_id, boarding_no, seat_no)
VALUES 
('ticket_1', 1, 1, '1'),
('ticket_2', 1, 2, '2'),
('ticket_3', 1, 3, '3'),
('ticket_4', 2, 4, '4'),
('ticket_5', 2, 5, '5'),
('ticket_6', 2, 6, '6'),
('ticket_7', 3, 7, '7'),
('ticket_8', 4, 8, '8'),
('ticket_9', 4, 9, '9'),
('ticket_10', 4, 10, '10'),
('ticket_11', 4, 11, '11'),
('ticket_12', 5, 12, '12'),
('ticket_13', 5, 13, '13'),
('ticket_14', 5, 14, '14'),
('ticket_15', 5, 15, '15');
