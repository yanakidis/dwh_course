CREATE TABLE bookings (
    book_ref char(6) PRIMARY KEY,
    book_date timestamptz NOT NULL,
    total_amount integer NOT NULL
);

CREATE TABLE tickets (
    ticket_no char(13) PRIMARY KEY,
    book_ref char(6) NOT NULL,
    passenger_id varchar(20) NOT NULL,
    passenger_name text NOT NULL,
    contact_data jsonb,
    FOREIGN KEY (book_ref) REFERENCES bookings (book_ref)
);

CREATE TABLE airports (
    airport_code char(3) PRIMARY KEY,
    airport_name text NOT NULL,
    city text NOT NULL,
    coordinates_lon double precision NOT NULL,
    coordinates_lat double precision NOT NULL,
    timezone text NOT NULL
);

CREATE TABLE aircrafts (
    aircraft_code char(3) PRIMARY KEY,
    model jsonb NOT NULL,
    range integer NOT NULL
);

CREATE TABLE seats (
    aircraft_code char(3) NOT NULL,
    seat_no varchar(4) NOT NULL,
    fare_conditions varchar(10) NOT NULL,
    PRIMARY KEY (aircraft_code, seat_no),
    FOREIGN KEY (aircraft_code) REFERENCES aircrafts (aircraft_code)
);

CREATE TABLE flights (
    flight_id serial PRIMARY KEY,
    flight_no char(6) NOT NULL,
    scheduled_departure timestamptz NOT NULL,
    scheduled_arrival timestamptz NOT NULL,
    departure_airport char(3) NOT NULL,
    arrival_airport char(3) NOT NULL,
    status varchar(20) NOT NULL,
    aircraft_code char(3) NOT NULL,
    actual_departure timestamptz,
    actual_arrival timestamptz,
    FOREIGN KEY (departure_airport) REFERENCES airports (airport_code),
    FOREIGN KEY (arrival_airport) REFERENCES airports (airport_code),
    FOREIGN KEY (aircraft_code) REFERENCES aircrafts (aircraft_code)
);

CREATE TABLE ticket_flights (
    ticket_no char(13) NOT NULL,
    flight_id integer NOT NULL,
    fare_conditions integer NOT NULL,
    amount integer NOT NULL,
    PRIMARY KEY (ticket_no, flight_id),
    FOREIGN KEY (ticket_no) REFERENCES tickets (ticket_no),
    FOREIGN KEY (flight_id) REFERENCES flights (flight_id)
);

CREATE TABLE boarding_passes (
    ticket_no char(13) NOT NULL,
    flight_id integer NOT NULL,
    boarding_no integer NOT NULL,
    seat_no varchar(4) NOT NULL,
    PRIMARY KEY (ticket_no, flight_id),
    FOREIGN KEY (ticket_no, flight_id) REFERENCES ticket_flights (ticket_no, flight_id)
);

ALTER TABLE public.bookings replica identity full;
ALTER TABLE public.tickets replica identity full;
ALTER TABLE public.airports replica identity full;
ALTER TABLE public.aircrafts replica identity full;
ALTER TABLE public.flights replica identity full;
ALTER TABLE public.ticket_flights replica identity full;
ALTER TABLE public.boarding_passes replica identity full;

-- INSERT INTO bookings
-- (book_ref, book_date, total_amount)
-- VALUES
-- ('ref_1', '2024-06-22 19:10:25-07', 3),
-- ('ref_2', '2024-07-22 19:11:25-07', 2),
-- ('ref_3', '2024-05-22 19:15:25-07', 1),
-- ('ref_4', '2024-04-22 19:10:25-07', 1),
-- ('ref_5', '2024-03-22 19:10:25-07', 2),
-- ('ref_6', '2024-02-22 19:10:25-07', 2),
-- ('ref_7', '2024-01-22 19:10:25-07', 4);
--
-- INSERT INTO tickets
-- (ticket_no, book_ref, passenger_id, passenger_name, contact_data)
-- VALUES
-- ('ticket_1', 'ref_1', 'pas_1', 'Mike', NULL),
-- ('ticket_2', 'ref_1', 'pas_2', 'Michael', NULL),
-- ('ticket_3', 'ref_1', 'pas_3', 'George', NULL),
-- ('ticket_4', 'ref_2', 'pas_4', 'Alex', NULL),
-- ('ticket_5', 'ref_2', 'pas_5', 'Bob', NULL),
-- ('ticket_6', 'ref_3', 'pas_2', 'Michael', NULL),
-- ('ticket_7', 'ref_4', 'pas_6', 'Victor', NULL),
-- ('ticket_8', 'ref_5', 'pas_7', 'Vlad', NULL),
-- ('ticket_9', 'ref_5', 'pas_8', 'Denni', NULL),
-- ('ticket_10', 'ref_6', 'pas_9', 'John', NULL),
-- ('ticket_11', 'ref_6', 'pas_10', 'Jorah', NULL),
-- ('ticket_12', 'ref_7', 'pas_1', 'Mike', NULL),
-- ('ticket_13', 'ref_7', 'pas_2', 'Michael', NULL),
-- ('ticket_14', 'ref_7', 'pas_5', 'Bob', NULL),
-- ('ticket_15', 'ref_7', 'pas_7', 'Vlad', NULL);
--
-- INSERT INTO airports
-- (airport_code, airport_name, city, coordinates_lon, coordinates_lat, timezone)
-- VALUES
-- ('MSK', 'Moscow', 'Moscow', 1, 23, '+3'),
-- ('SPB', 'Saint Petersburg', 'Saint Petersburg', 12, 12, '+3'),
-- ('IST', 'Istanbul', 'Istanbul', 2, 41, '+3'),
-- ('KZN', 'Kazan', 'Kazan', 3, 123, '+4');
--
-- INSERT INTO aircrafts
-- (aircraft_code, model, range)
-- VALUES
-- ('001', '{}', 1),
-- ('002', '{}', 2),
-- ('003', '{}', 3),
-- ('004', '{}', 4),
-- ('005', '{}', 5);
--
-- INSERT INTO flights
-- (flight_id, flight_no, scheduled_departure, scheduled_arrival, departure_airport, arrival_airport, status, aircraft_code, actual_departure, actual_arrival)
-- VALUES
-- (1, '1', '2024-06-22 19:10:25-07', '2024-06-22 19:10:25-07', 'MSK', 'SPB', 'NULL', '001', '2024-06-22 19:10:25-07', '2024-06-22 19:10:25-07'),
-- (2, '2', '2024-06-22 19:10:25-07', '2024-06-22 19:10:25-07', 'MSK', 'IST', 'NULL', '002', '2024-06-22 19:10:25-07', '2024-06-22 19:10:25-07'),
-- (3, '3', '2024-06-22 19:10:25-07', '2024-06-22 19:10:25-07', 'SPB', 'KZN', 'NULL', '003', '2024-06-22 19:10:25-07', '2024-06-22 19:10:25-07'),
-- (4, '4', '2024-06-22 19:10:25-07', '2024-06-22 19:10:25-07', 'KZN', 'IST', 'NULL', '004', '2024-06-22 19:10:25-07', '2024-06-22 19:10:25-07'),
-- (5, '5', '2024-06-22 19:10:25-07', '2024-06-22 19:10:25-07', 'IST', 'SPB', 'NULL', '005', '2024-06-22 19:10:25-07', '2024-06-22 19:10:25-07');
--
-- INSERT INTO ticket_flights (ticket_no, flight_id, fare_conditions, amount)
-- VALUES
-- ('ticket_1', 1, 0, 100),
-- ('ticket_2', 1, 0, 200),
-- ('ticket_3', 1, 0, 1000),
-- ('ticket_4', 2, 0, 300),
-- ('ticket_5', 2, 0, 432),
-- ('ticket_6', 2, 0, 625),
-- ('ticket_7', 3, 0, 2345),
-- ('ticket_8', 4, 0, 234),
-- ('ticket_9', 4, 0, 134),
-- ('ticket_10', 4, 0, 75),
-- ('ticket_11', 4, 0, 134),
-- ('ticket_12', 5, 0, 363),
-- ('ticket_13', 5, 0, 534),
-- ('ticket_14', 5, 0, 764),
-- ('ticket_15', 5, 0, 632);




