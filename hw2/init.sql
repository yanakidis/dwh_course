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