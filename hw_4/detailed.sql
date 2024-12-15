/* https://habr.com/ru/articles/829338/ - исследовал готовый рецепт, очень качественный */

/* Схема для составляющих Data Vault */

DROP SCHEMA IF EXISTS dwh_detailed CASCADE;
CREATE SCHEMA IF NOT EXISTS dwh_detailed;

/* DDL для таблиц Hub */

-- Для начала удалим таблицы, если такие имеются.
DROP TABLE IF EXISTS dwh_detailed.Hub_flights CASCADE;
DROP TABLE IF EXISTS dwh_detailed.Hub_airports_data CASCADE;
DROP TABLE IF EXISTS dwh_detailed.Hub_aircrafts_data CASCADE;
DROP TABLE IF EXISTS dwh_detailed.Hub_seats CASCADE;
DROP TABLE IF EXISTS dwh_detailed.Hub_ticket_flights CASCADE;
DROP TABLE IF EXISTS dwh_detailed.Hub_boarding_passes CASCADE;
DROP TABLE IF EXISTS dwh_detailed.Hub_tickets CASCADE;
DROP TABLE IF EXISTS dwh_detailed.Hub_bookings CASCADE;

/* функция для формирования таблиц Hub */
CREATE OR REPLACE FUNCTION dwh_detailed.ddl_hub_table(      
               table_name TEXT , -- название таблицы Hub
               business_key TEXT) -- какие колонки надо вытащить из таблицы источника указывается с форматом
               RETURNS VOID AS $$
DECLARE 
     qwery TEXT;
BEGIN -- проверка на наличие данных в параметрах функции
    IF 
	  (table_name IS NULL) OR (business_key IS NULL)
		THEN RAISE EXCEPTION 'no data';
	END IF;
qwery:= 'CREATE TABLE IF NOT EXISTS dwh_detailed.' || table_name || '(' || 
        'Hash_key varchar(33) PRIMARY KEY,
         record_source varchar(20) NOT NULL,
         Load_date timestamptz NOT NULL ,' ||
         business_key || ' NOT NULL);';
    EXECUTE qwery;
END;
$$ LANGUAGE plpgsql;


-- flights
SELECT dwh_detailed.ddl_hub_table('Hub_flights', 'flight_id bigint');

-- airports
SELECT dwh_detailed.ddl_hub_table('Hub_airports_data', 'airport_code bpchar(3)');

-- aircrafts
SELECT dwh_detailed.ddl_hub_table('Hub_aircrafts_data', 'aircraft_code bpchar(3)');

-- seats
SELECT dwh_detailed.ddl_hub_table('Hub_seats', 'aircraft_code_seat_no varchar(20)');

-- ticket_flights
SELECT dwh_detailed.ddl_hub_table('Hub_ticket_flights', 'ticket_no_flight_id varchar(25)');

-- boarding_passes
SELECT dwh_detailed.ddl_hub_table('Hub_boarding_passes', 'ticket_no_flight_id varchar(25)');

-- tickets
SELECT dwh_detailed.ddl_hub_table('Hub_tickets', 'ticket_no varchar(20)');

-- bookings
SELECT dwh_detailed.ddl_hub_table('Hub_bookings', 'book_ref varchar(15)');

/* DDL для таблиц Link */

-- Для начала удалим таблицы, если такие имеются.
DROP TABLE IF EXISTS dwh_detailed.Link_flights_airport_data_departure CASCADE;
DROP TABLE IF EXISTS dwh_detailed.Link_flights_airport_data_arrival CASCADE;
DROP TABLE IF EXISTS dwh_detailed.Link_flights_ticket_flights CASCADE;
DROP TABLE IF EXISTS dwh_detailed.Link_ticket_flights_boarding_passes CASCADE;
DROP TABLE IF EXISTS dwh_detailed.Link_ticket_flights_tickets CASCADE;
DROP TABLE IF EXISTS dwh_detailed.Link_tickets_bookings CASCADE;
DROP TABLE IF EXISTS dwh_detailed.Link_flights_aircrafts_data CASCADE;
DROP TABLE IF EXISTS dwh_detailed.Link_seats_aircrafts_data CASCADE;

/* Функция DDL Link */
CREATE OR REPLACE FUNCTION dwh_detailed.ddl_Link_table(
               table_name TEXT, -- название таблицы линка
               hub_hash_1 TEXT, -- название хеша хаба 1, к которому относится линк
               hub_hash_2 TEXT) -- название хеша хаба 2, к которому относится линк)
               RETURNS VOID AS $$
DECLARE
     qwery TEXT;
BEGIN -- проверка на наличие данных в параметрах функции
    IF
	  (table_name IS NULL) OR (hub_hash_1 IS NULL) OR (hub_hash_2 IS NULL)
		THEN RAISE EXCEPTION 'no data';
	END IF;
qwery:= 'CREATE TABLE IF NOT EXISTS dwh_detailed.' || table_name || '(' ||
         table_name || '_Hash_key varchar(33),' ||
         'load_date timestamptz, load_end_date timestamptz, record_source varchar(20), ' ||
         hub_hash_1 || '_Hash_key varchar(33) REFERENCES dwh_detailed.' || hub_hash_1 || '(Hash_key),' ||
         hub_hash_2 || '_Hash_key varchar(33) REFERENCES dwh_detailed.' || hub_hash_2 || '(Hash_key));';
    EXECUTE qwery;
END;
$$ LANGUAGE plpgsql;

---- flights_airport_data_departure
SELECT dwh_detailed.ddl_Link_table('Link_flights_airport_data_departure', 'Hub_flights', 'Hub_airports_data');

---- flights_airport_arrival_airport
SELECT dwh_detailed.ddl_Link_table('Link_flights_airport_data_arrival', 'Hub_flights', 'Hub_airports_data');

---- flights_ticket_flights
SELECT dwh_detailed.ddl_Link_table('Link_flights_ticket_flights', 'Hub_flights', 'Hub_ticket_flights');

---- ticket_flights_boarding_passes
SELECT dwh_detailed.ddl_Link_table('Link_ticket_flights_boarding_passes', 'Hub_ticket_flights', 'Hub_boarding_passes');

---- ticket_flights_tickets
SELECT dwh_detailed.ddl_Link_table('Link_ticket_flights_tickets', 'Hub_ticket_flights', 'Hub_tickets');

---- tickets_bookings
SELECT dwh_detailed.ddl_Link_table('Link_tickets_bookings', 'Hub_tickets', 'Hub_bookings');

---- flights_aircrafts_data
SELECT dwh_detailed.ddl_Link_table('Link_flights_aircrafts_data', 'Hub_flights', 'Hub_aircrafts_data');

---- seats_aircrafts_data
SELECT dwh_detailed.ddl_Link_table('Link_seats_aircrafts_data', 'Hub_seats', 'Hub_aircrafts_data');

/* DDL для таблиц Satellite */

-- Чистим чистим
DROP TABLE IF exists dwh_detailed.Sattelite_aircrafts_data_range;
DROP TABLE IF exists dwh_detailed.Sattelite_aircrafts_data_model;
DROP TABLE IF exists dwh_detailed.Sattelite_seats_fare_conditions;
DROP TABLE IF exists dwh_detailed.Sattelite_airport_data_timezone;
DROP TABLE IF exists dwh_detailed.Sattelite_airport_data_coordinates;
DROP TABLE IF exists dwh_detailed.Sattelite_airport_data_airport_name;
DROP TABLE IF exists dwh_detailed.Sattelite_airport_data_city;
DROP TABLE IF exists dwh_detailed.Sattelite_ticket_flights_amount;
DROP TABLE IF exists dwh_detailed.Sattelite_ticket_flights_fare_conditions;
DROP TABLE IF exists dwh_detailed.Sattelite_boarding_passes_boarding_no;
DROP TABLE IF exists dwh_detailed.Sattelite_boarding_passes_seat_no;
DROP TABLE IF exists dwh_detailed.Sattelite_tickets_book_ref;
DROP TABLE IF exists dwh_detailed.Sattelite_tickets_contact_data;
DROP TABLE IF exists dwh_detailed.Sattelite_tickets_passenger_id;
DROP TABLE IF exists dwh_detailed.Sattelite_tickets_passenger_name;
DROP TABLE IF exists dwh_detailed.Sattelite_bookings_book_date;
DROP TABLE IF exists dwh_detailed.Sattelite_bookings_total_amount;
DROP TABLE IF exists dwh_detailed.Sattelite_flights_flight_no;
DROP TABLE IF exists dwh_detailed.Sattelite_flights_scheduled_departure;
DROP TABLE IF exists dwh_detailed.Sattelite_flights_scheduled_arrival;
DROP TABLE IF exists dwh_detailed.Sattelite_flights_departure_airport;
DROP TABLE IF exists dwh_detailed.Sattelite_flights_arrival_airport;
DROP TABLE IF exists dwh_detailed.Sattelite_flights_status;
DROP TABLE IF exists dwh_detailed.Sattelite_flights_aircraft_code;
DROP TABLE IF exists dwh_detailed.Sattelite_flights_actual_departure;
DROP TABLE IF exists dwh_detailed.Sattelite_flights_actual_arrival;

/* Функция DDL Satellite */
CREATE OR REPLACE FUNCTION dwh_detailed.ddl_Sattelite_table(      
               table_name TEXT, -- название таблицы сателлита
               hub_hash TEXT, -- название хеша хаба, к которому относится сателлит
               atrib TEXT) -- какие колонки надо вытащить из таблицы источника указывается вместе с форматом
               RETURNS VOID AS $$
DECLARE 
     qwery TEXT;
BEGIN -- проверка на наличие данных в параметрах функции
    IF 
	  (table_name IS NULL) OR (hub_hash IS NULL) OR (atrib IS NULL)
		THEN RAISE EXCEPTION 'no data';
	END IF;
qwery:= 'CREATE TABLE IF NOT EXISTS dwh_detailed.' || table_name || '(' || 
         hub_hash || '_Hash_key varchar(33)' || ' REFERENCES dwh_detailed.' || hub_hash || '(Hash_key),' ||
         'load_date timestamptz, load_end_date timestamptz, record_source varchar(20), ' || atrib || ', ' ||
         'PRIMARY KEY (' || hub_hash || '_Hash_key ' || ', load_date));';
    EXECUTE qwery;  
END;
$$ LANGUAGE plpgsql;

/*Sattelite_aircrafts_data*/
-- range
SELECT dwh_detailed.ddl_Sattelite_table('Sattelite_aircrafts_data_range', 'Hub_aircrafts_data', 'range int4');
--model
SELECT dwh_detailed.ddl_Sattelite_table('Sattelite_aircrafts_data_model', 'Hub_aircrafts_data', 'model TEXT');

/*Sattelite_seats*/
-- fare_conditions
SELECT dwh_detailed.ddl_Sattelite_table('Sattelite_seats_fare_conditions', 'Hub_seats', 'fare_conditions varchar(15)');

/*Sattelite_airoport_data*/
-- timezone
SELECT dwh_detailed.ddl_Sattelite_table('Sattelite_airport_data_timezone', 'Hub_airports_data', 'timezone varchar(34)');
-- coordinates_lon
SELECT dwh_detailed.ddl_Sattelite_table('Sattelite_airport_data_coordinates_lon', 'Hub_airports_data', 'coordinates_lon double precision');
-- coordinates_lat
SELECT dwh_detailed.ddl_Sattelite_table('Sattelite_airport_data_coordinates_lat', 'Hub_airports_data', 'coordinates_lat double precision');
-- airport_name
SELECT dwh_detailed.ddl_Sattelite_table('Sattelite_airport_data_airport_name', 'Hub_airports_data', 'airport_name text');
-- city
SELECT dwh_detailed.ddl_Sattelite_table('Sattelite_airport_data_city', 'Hub_airports_data', 'city text');

/*Sattelite_ticket_flights*/
-- amount
SELECT dwh_detailed.ddl_Sattelite_table('Sattelite_ticket_flights_amount', 'Hub_ticket_flights', 'amount numeric(10, 2)');
-- fare_conditions
SELECT dwh_detailed.ddl_Sattelite_table('Sattelite_ticket_flights_fare_conditions', 'Hub_ticket_flights', 'fare_conditions varchar(10)');

/*Sattelite_boarding_passes*/
-- boarding_no
SELECT dwh_detailed.ddl_Sattelite_table('Sattelite_boarding_passes_boarding_no', 'Hub_boarding_passes', 'boarding_no int4');
-- seat_no
SELECT dwh_detailed.ddl_Sattelite_table('Sattelite_boarding_passes_seat_no', 'Hub_boarding_passes', 'seat_no varchar(4)');

/*Sattelite_tickets*/
-- book_ref
SELECT dwh_detailed.ddl_Sattelite_table('Sattelite_tickets_book_ref', 'Hub_tickets', 'book_ref bpchar(6)');
-- contact_data
SELECT dwh_detailed.ddl_Sattelite_table('Sattelite_tickets_contact_data', 'Hub_tickets', 'contact_data jsonb');
-- passenger_id
SELECT dwh_detailed.ddl_Sattelite_table('Sattelite_tickets_passenger_id', 'Hub_tickets', 'passenger_id varchar(20)');
-- passenger_name
SELECT dwh_detailed.ddl_Sattelite_table('Sattelite_tickets_passenger_name', 'Hub_tickets', 'passenger_name text');

/*Sattelite_bookings*/
-- book_date
SELECT dwh_detailed.ddl_Sattelite_table('Sattelite_bookings_book_date', 'Hub_bookings', 'book_date timestamptz');
-- total_amount
SELECT dwh_detailed.ddl_Sattelite_table('Sattelite_bookings_total_amount', 'Hub_bookings', 'total_amount numeric(10, 2)');

/*Sattelite_flights*/
-- flight_no
SELECT dwh_detailed.ddl_Sattelite_table('Sattelite_flights_flight_no', 'Hub_flights', 'flight_no bpchar(6)');
-- scheduled_departure
SELECT dwh_detailed.ddl_Sattelite_table('Sattelite_flights_scheduled_departure', 'Hub_flights', 'scheduled_departure timestamptz');
-- scheduled_arrival
SELECT dwh_detailed.ddl_Sattelite_table('Sattelite_flights_scheduled_arrival', 'Hub_flights', 'scheduled_arrival timestamptz');
-- departure_airport
SELECT dwh_detailed.ddl_Sattelite_table('Sattelite_flights_departure_airport', 'Hub_flights', 'departure_airport bpchar(3)');
-- arrival_airport
SELECT dwh_detailed.ddl_Sattelite_table('Sattelite_flights_arrival_airport', 'Hub_flights', 'arrival_airport bpchar(3)');
-- status
SELECT dwh_detailed.ddl_Sattelite_table('Sattelite_flights_status', 'Hub_flights', 'status varchar(20)');
-- aircraft_code
SELECT dwh_detailed.ddl_Sattelite_table('Sattelite_flights_aircraft_code', 'Hub_flights', 'aircraft_code bpchar(3)');
-- actual_departure
SELECT dwh_detailed.ddl_Sattelite_table('Sattelite_flights_actual_departure', 'Hub_flights', 'actual_departure timestamptz');
-- actual_arrival
SELECT dwh_detailed.ddl_Sattelite_table('Sattelite_flights_actual_arrival', 'Hub_flights', 'actual_arrival timestamptz');
