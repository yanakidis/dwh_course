import json
import hashlib
from kafka import KafkaConsumer
from sqlalchemy import create_engine, text
import os
import logging

logging.basicConfig(level=logging.INFO)


logging.info("HELLO #1")

KAFKA_HOST = "broker"
KAFKA_CONSUMER_GROUP = "backend"
POSTGRES_URI = "postgresql://postgres:postgres@postgres_dwh:5432/postgres"

os.system("curl -X POST --location 'http://debezium:8083/connectors' -H 'Content-Type: application/json' -H 'Accept: application/json' -d @connector.json")
os.system("curl http://rest-proxy:8082/v3/clusters -o clusters.json")

with open('clusters.json', 'r') as f:
    clusters = json.load(f)

cls_id = clusters['data'][0]['cluster_id']
variable = "http://rest-proxy:8082/v3/clusters/" + cls_id + "/topics"
os.system(f"curl -v '{variable}' -o topics.json")

with open('topics.json', 'r') as f:
    topics = json.load(f)

logging.info("CHECK #1")

engine = create_engine(POSTGRES_URI)

logging.info("CHECK #2")

consumer = KafkaConsumer(
    #bootstrap_servers=f"{KAFKA_HOST}:9092",
    bootstrap_servers=f"{KAFKA_HOST}:29092",
    value_deserializer=lambda v: v if v is None else json.loads( v.decode("utf-8") ),
    auto_offset_reset="earliest",
    group_id=KAFKA_CONSUMER_GROUP,
    api_version=(0,10)
)

logging.info("CHECK #3")

consumer.subscribe(
    topics=[
        "postgres.public.bookings", "postgres.public.tickets", "postgres.public.airports", 
        "postgres.public.aircrafts", "postgres.public.seats", "postgres.public.flights",
        "postgres.public.ticket_flights", "postgres.public.boarding_passes"
    ]
)


def concat_hash(keys):
    con_keys = "|".join(map(str, keys))
    return hashlib.md5(con_keys.encode()).hexdigest()


def update_query_in_update_operation(table, key_condition):
    return f"""
        UPDATE dwh_detailed.{table}
        SET load_end_date = to_timestamp(:time)
        WHERE {key_condition}
            AND load_date = (
                SELECT MAX(load_date)
                FROM dwh_detailed.{table}
                WHERE {key_condition}
                AND load_date < to_timestamp(:time)
            );
    """


def update_query_in_delete_operation(table, key_condition):
    return f"""
        UPDATE dwh_detailed.{table}
        SET load_end_date = to_timestamp(:time)
        WHERE {key_condition}
            AND load_date = (
                SELECT MAX(load_date)
                FROM dwh_detailed.{table}
                WHERE {key_condition}
            );
    """


def insert_into_hub(table, source, key):
    return f"""
        INSERT INTO dwh_detailed.{table} (hash_key, record_source, load_date, {key}) 
        VALUES (MD5((:{key})::varchar), '{source}', to_timestamp(:time), :{key})
        ON CONFLICT DO NOTHING;
    """


def insert_into_sattelite(table, source, key_column_title, key, column):
    return f"""
        INSERT INTO dwh_detailed.{table} ({key_column_title}, load_date, load_end_date, record_source, {column})
        VALUES (MD5((:{key})::varchar), to_timestamp(:time), null, '{source}', :{column});
    """


def insert_into_link(table, source, link_key_title, hub_key_title_1, hub_key_title_2, link_key, hub_key_1, hub_key_2):
    return f"""
        INSERT INTO dwh_detailed.{table} ({link_key_title}, load_date, load_end_date, record_source, {hub_key_title_1}, {hub_key_title_2})
        VALUES (((:{link_key})::varchar), to_timestamp(:time), null, '{source}', MD5((:{hub_key_1})::varchar), MD5((:{hub_key_2})::varchar));
    """


def process_bookings(operation, before, after, ts):
    with engine.begin() as conn:
        # все связанные с этой таблицей таблицы в dwh
        # Hub_bookings  
        # Sattelite_bookings_book_date
        # Sattelite_bookings_total_amount

        all_keys_after = None
        all_keys_before = None
        if operation == "insert" or operation == "update":
            all_keys_after = {
                "book_ref": after["book_ref"],
                "time": ts,
                "book_date": after["book_date"],
                "total_amount": after["total_amount"],
            }
        elif operation == "delete":
            all_keys_before = {
                "book_ref": before["book_ref"],
                "time": ts,
                "book_date": before["book_date"],
                "total_amount": before["total_amount"],
            }
        
        sat_tables = ["Sattelite_bookings_book_date", "Sattelite_bookings_total_amount"]
        sat_columns =["book_date", "total_amount"]

        if operation == "insert":
            # добавляем запись в хаб, а атрибуты в сателиты
            conn.execute(
                text(insert_into_hub("Hub_bookings", "bookings", "book_ref")), 
                all_keys_after
            )

            for i in range(len(sat_tables)):
                conn.execute(
                    text(insert_into_sattelite(sat_tables[i], "bookings", "hub_bookings_hash_key", "book_ref", sat_columns[i])), 
                    all_keys_after
                )

        elif operation == "update":
            for i in range(len(sat_tables)):
                conn.execute(
                    text(insert_into_sattelite(sat_tables[i], "bookings", "hub_bookings_hash_key", "book_ref", sat_columns[i])), 
                    all_keys_after
                )

                # обновляем end_date в самой последней записе в сателите
                conn.execute(text(
                    update_query_in_update_operation(
                        sat_tables[i], 
                        "hub_bookings_hash_key = MD5((:book_ref)::varchar)"
                    )), all_keys_after
                )

        elif operation == "delete":
            # обновляем в сателитах end_date
            for i in range(len(sat_tables)):
                conn.execute(text(
                    update_query_in_delete_operation(
                        sat_tables[i], 
                        "hub_bookings_hash_key = MD5((:book_ref)::varchar)"
                    )), all_keys_before
                )


def process_tickets(operation, before, after, ts):
    with engine.begin() as conn:
        # все связанные с этой таблицей таблицы в dwh
        # Hub_tickets
        # Sattelite_tickets_book_ref
        # Sattelite_tickets_contact_data
        # Sattelite_tickets_passenger_id
        # Sattelite_tickets_passenger_name
        # Link_tickets_bookings

        all_keys_after = None
        all_keys_before = None
        if operation == "insert" or operation == "update":
            all_keys_after = {
                "ticket_no": after["ticket_no"],
                "time": ts,
                "book_ref": after["book_ref"],
                "contact_data": after["contact_data"],
                "passenger_id": after["passenger_id"],
                "passenger_name": after["passenger_name"],
                "link_hash": concat_hash([after["ticket_no"], after["book_ref"]]),
            }
        elif operation == "delete":
            all_keys_before = {
                "ticket_no": before["ticket_no"],
                "time": ts,
                "book_ref": before["book_ref"],
                "contact_data": before["contact_data"],
                "passenger_id": before["passenger_id"],
                "passenger_name": before["passenger_name"],
                "link_hash": concat_hash([before["ticket_no"], before["book_ref"]]),
            }
        
        sat_tables = ["Sattelite_tickets_book_ref", "Sattelite_tickets_contact_data", "Sattelite_tickets_passenger_id", "Sattelite_tickets_passenger_name"]
        sat_columns = ["book_ref", "contact_data", "passenger_id", "passenger_name"]

        if operation == "insert":
            # добавляем запись в хаб
            conn.execute(
                text(insert_into_hub("Hub_tickets", "tickets", "ticket_no")), 
                all_keys_after
            )

            # атрибуты добавляем в сателиты
            for i in range(len(sat_tables)):
                conn.execute(
                    text(insert_into_sattelite(sat_tables[i], "tickets", "hub_tickets_hash_key", "ticket_no", sat_columns[i])), 
                    all_keys_after
                )

            # теперь добавляем линк tickets с bookings
            conn.execute(
                text(insert_into_link(
                    "Link_tickets_bookings", "tickets", "link_tickets_bookings_hash_key", "hub_tickets_hash_key", 
                    "hub_bookings_hash_key", "link_hash", "ticket_no", "book_ref"
                )), 
                all_keys_after
            )
        elif operation == "update":
            # обновляем сателиты (новая строчка + в самой последней записи обновляем end_date), а затем линки

            for i in range(len(sat_tables)):
                conn.execute(
                    text(insert_into_sattelite(sat_tables[i], "tickets", "hub_tickets_hash_key", "ticket_no", sat_columns[i])), 
                    all_keys_after
                )

                conn.execute(text(
                    update_query_in_update_operation(
                        sat_tables[i], 
                        "hub_tickets_hash_key = MD5((:ticket_no)::varchar)"
                    )), all_keys_after
                )

            # теперь добавим запись и изменим самую последнюю (end_date) в линк
            conn.execute(
                text(insert_into_link(
                    "Link_tickets_bookings", "tickets", "link_tickets_bookings_hash_key", "hub_tickets_hash_key", 
                    "hub_bookings_hash_key", "link_hash", "ticket_no", "book_ref"
                )), 
                all_keys_after
            )

            conn.execute(text(
                update_query_in_update_operation(
                    "Link_tickets_bookings", 
                    "link_tickets_bookings_hash_key = ((:link_hash)::varchar)"
                )), all_keys_after
            )
        
        elif operation == "delete":
            # обновляем в сателитах end_date
            for i in range(len(sat_tables)):
                conn.execute(text(
                    update_query_in_delete_operation(
                        sat_tables[i], 
                        "hub_tickets_hash_key = MD5((:ticket_no)::varchar)"
                    )), all_keys_before
                )

            # и обновим end_date в линке
            conn.execute(text(
                update_query_in_delete_operation(
                    "Link_tickets_bookings", 
                    "link_tickets_bookings_hash_key = ((:link_hash)::varchar)"
                )), all_keys_before
            )


def process_airports(operation, before, after, ts):
    with engine.begin() as conn:
        # все связанные с этой таблицей таблицы в dwh
        # Hub_airports_data
        # Sattelite_airport_data_timezone
        # Sattelite_airport_data_coordinates_lon
        # Sattelite_airport_data_coordinates_lat
        # Sattelite_airport_data_airport_name
        # Sattelite_airport_data_city

        all_keys_after = None
        all_keys_before = None
        if operation == "insert" or operation == "update":
            all_keys_after = {
                "airport_code": after["airport_code"],
                "time": ts,
                "timezone": after["timezone"],
                "coordinates_lon": after["coordinates_lon"],
                "coordinates_lat": after["coordinates_lat"],
                "airport_name": after["airport_name"],
                "city": after["city"],
            }
        elif operation == "delete":
            all_keys_before = {
                "airport_code": before["airport_code"],
                "time": ts,
                "timezone": before["timezone"],
                "coordinates_lon": before["coordinates_lon"],
                "coordinates_lat": before["coordinates_lat"],
                "airport_name": after["airport_name"],
                "city": before["city"],
            }

        sat_tables = ["Sattelite_airport_data_timezone", "Sattelite_airport_data_coordinates_lon", "Sattelite_airport_data_coordinates_lat", "Sattelite_airport_data_airport_name", "Sattelite_airport_data_city"]
        sat_columns = ["timezone", "coordinates_lon", "coordinates_lat", "airport_name", "city"]

        if operation == "insert":
            # добавляем запись в хаб
            conn.execute(
                text(insert_into_hub("Hub_airports_data", "airports", "airport_code")), 
                all_keys_after
            )

            # атрибуты добавляем в сателиты
            for i in range(len(sat_tables)):
                conn.execute(
                    text(insert_into_sattelite(sat_tables[i], "airports", "hub_airports_data_hash_key", "airport_code", sat_columns[i])), 
                    all_keys_after
                )
        
        elif operation == "update":
            # обновляем сателиты (новая строчка + в самой последней записи обновляем end_date), а затем линки

            for i in range(len(sat_tables)):
                conn.execute(
                    text(insert_into_sattelite(sat_tables[i], "airports", "hub_airports_data_hash_key", "airport_code", sat_columns[i])), 
                    all_keys_after
                )

                conn.execute(text(
                    update_query_in_update_operation(
                        sat_tables[i], 
                        "hub_airports_data_hash_key = MD5((:airport_code)::varchar)"
                    )), all_keys_after
                )

        elif operation == "delete":
            # обновляем в сателитах end_date
            for i in range(len(sat_tables)):
                conn.execute(text(
                    update_query_in_delete_operation(
                        sat_tables[i], 
                        "hub_airports_data_hash_key = MD5((:airport_code)::varchar)"
                    )), all_keys_before
                )


def process_aircrafts(operation, before, after, ts):
    with engine.begin() as conn:
        # все связанные с этой таблицей таблицы в dwh
        # Hub_aircrafts_data
        # Sattelite_aircrafts_data_range
        # Sattelite_aircrafts_data_model

        all_keys_after = None
        all_keys_before = None
        if operation == "insert" or operation == "update":
            all_keys_after = {
                "aircraft_code": after["aircraft_code"],
                "time": ts,
                "range": after["range"],
                "model": after["model"],
            }
        elif operation == "delete":
            all_keys_before = {
                "aircraft_code": before["aircraft_code"],
                "time": ts,
                "range": before["range"],
                "model": before["model"],
            }
        
        sat_tables = ["Sattelite_aircrafts_data_range", "Sattelite_aircrafts_data_model"]
        sat_columns = ["range", "model"]
        
        if operation == "insert":
            # добавляем запись в хаб
            conn.execute(
                text(insert_into_hub("Hub_aircrafts_data", "aircrafts", "aircraft_code")), 
                all_keys_after
            )

            # атрибуты добавляем в сателиты
            for i in range(len(sat_tables)):
                conn.execute(
                    text(insert_into_sattelite(sat_tables[i], "aircrafts", "hub_aircrafts_data_hash_key", "aircraft_code", sat_columns[i])), 
                    all_keys_after
                )
        
        elif operation == "update":
            # обновляем сателиты (новая строчка + в самой последней записи обновляем end_date), а затем линки

            for i in range(len(sat_tables)):
                conn.execute(
                    text(insert_into_sattelite(sat_tables[i], "aircrafts", "hub_aircrafts_data_hash_key", "aircraft_code", sat_columns[i])), 
                    all_keys_after
                )

                conn.execute(text(
                    update_query_in_update_operation(
                        sat_tables[i], 
                        "hub_aircrafts_data_hash_key = MD5((:aircraft_code)::varchar)"
                    )), all_keys_after
                )
        
        elif operation == "delete":
            # обновляем в сателитах end_date
            for i in range(len(sat_tables)):
                conn.execute(text(
                    update_query_in_delete_operation(
                        sat_tables[i], 
                        "hub_aircrafts_data_hash_key = MD5((:aircraft_code)::varchar)"
                    )), all_keys_before
                )


def process_seats(operation, before, after, ts):
    with engine.begin() as conn:
        # все связанные с этой таблицей таблицы в dwh
        # Hub_seats
        # Sattelite_seats_fare_conditions
        # Link_seats_aircrafts_data

        all_keys_after = None
        all_keys_before = None
        if operation == "insert" or operation == "update":
            all_keys_after = {
                "aircraft_code_seat_no": str(after["aircraft_code"]) + "," + str(after["seat_no"]),
                "time": ts,
                "fare_conditions": after["fare_conditions"],
                "link_hash": concat_hash([str(after["aircraft_code"]) + "," + str(after["seat_no"]), after["aircraft_code"]]),
                "aircraft_code": after["aircraft_code"],
            }
        elif operation == "delete":
            all_keys_before = {
                "aircraft_code_seat_no": str(before["aircraft_code"]) + "," + str(before["seat_no"]),
                "time": ts,
                "fare_conditions": before["fare_conditions"],
                "link_hash": concat_hash([str(before["aircraft_code"]) + "," + str(before["seat_no"]), before["aircraft_code"]]),
                "aircraft_code": before["aircraft_code"]
            }

        sat_tables = ["Sattelite_seats_fare_conditions"]
        sat_columns = ["fare_conditions"]
        
        if operation == "insert":
            # добавляем запись в хаб
            conn.execute(
                text(insert_into_hub("Hub_seats", "seats", "aircraft_code_seat_no")), 
                all_keys_after
            )

            # атрибуты добавляем в сателиты
            for i in range(len(sat_tables)):
                conn.execute(
                    text(insert_into_sattelite(sat_tables[i], "seats", "hub_seats_hash_key", "aircraft_code_seat_no", sat_columns[i])), 
                    all_keys_after
                )

            # теперь добавляем линк seats с aircrafts
            conn.execute(
                text(insert_into_link(
                    "Link_seats_aircrafts_data", "seats", "link_seats_aircrafts_data_hash_key", "hub_seats_hash_key", 
                    "hub_aircrafts_data_hash_key", "link_hash", "aircraft_code_seat_no", "aircraft_code"
                )), 
                all_keys_after
            )
        
        elif operation == "update":
            # обновляем сателиты (новая строчка + в самой последней записи обновляем end_date), а затем линки

            for i in range(len(sat_tables)):
                conn.execute(
                    text(insert_into_sattelite(sat_tables[i], "seats", "hub_seats_hash_key", "aircraft_code_seat_no", sat_columns[i])), 
                    all_keys_after
                )

                conn.execute(text(
                    update_query_in_update_operation(
                        sat_tables[i], 
                        "hub_seats_hash_key = MD5((:aircraft_code_seat_no)::varchar)"
                    )), all_keys_after
                )

            # теперь добавим запись и изменим самую последнюю (end_date) в линк
            conn.execute(
                text(insert_into_link(
                    "Link_seats_aircrafts_data", "seats", "link_seats_aircrafts_data_hash_key", "hub_seats_hash_key", 
                    "hub_aircrafts_data_hash_key", "link_hash", "aircraft_code_seat_no", "aircraft_code"
                )), 
                all_keys_after
            )

            conn.execute(text(
                update_query_in_update_operation(
                    "Link_seats_aircrafts_data", 
                    "link_seats_aircrafts_data_hash_key = ((:link_hash)::varchar)"
                )), all_keys_after
            )
        
        elif operation == "delete":
            # обновляем в сателитах end_date
            for i in range(len(sat_tables)):
                conn.execute(text(
                    update_query_in_delete_operation(
                        sat_tables[i], 
                        "hub_seats_hash_key = MD5((:aircraft_code_seat_no)::varchar)"
                    )), all_keys_before
                )

            # и обновим end_date в линке
            conn.execute(text(
                update_query_in_delete_operation(
                    "Link_seats_aircrafts_data", 
                    "link_seats_aircrafts_data_hash_key = ((:link_hash)::varchar)"
                )), all_keys_before
            )


def process_boarding_passes(operation, before, after, ts):
    with engine.begin() as conn:
        # все связанные с этой таблицей таблицы в dwh
        # Hub_boarding_passes
        # Sattelite_boarding_passes_boarding_no
        # Sattelite_boarding_passes_seat_no
        # Link_ticket_flights_boarding_passes

        all_keys_after = None
        all_keys_before = None
        if operation == "insert" or operation == "update":
            all_keys_after = {
                "ticket_no_flight_id": str(after["ticket_no"]) + "," + str(after["flight_id"]),
                "time": ts,
                "boarding_no": after["boarding_no"],
                "seat_no": after["seat_no"],
                "link_hash": concat_hash([after["ticket_no"], after["flight_id"]]),
                "ticket_no": after["ticket_no"],
            }
        elif operation == "delete":
            all_keys_before = {
                "ticket_no_flight_id": str(before["ticket_no"]) + "," + str(before["flight_id"]),
                "time": ts,
                "boarding_no": before["boarding_no"],
                "seat_no": before["seat_no"],
                "link_hash": concat_hash([before["ticket_no"], before["flight_id"]]),
                "ticket_no": before["ticket_no"],
            }
        
        sat_tables = ["Sattelite_boarding_passes_boarding_no", "Sattelite_boarding_passes_seat_no"]
        sat_columns = ["boarding_no", "seat_no"]
        
        if operation == "insert":
            # добавляем запись в хаб
            conn.execute(
                text(insert_into_hub("Hub_boarding_passes", "boarding_passes", "ticket_no_flight_id")), 
                all_keys_after
            )

            # атрибуты добавляем в сателиты
            for i in range(len(sat_tables)):
                conn.execute(
                    text(insert_into_sattelite(sat_tables[i], "boarding_passes", "hub_boarding_passes_hash_key", "ticket_no_flight_id", sat_columns[i])), 
                    all_keys_after
                )

            # теперь добавляем линк tickets с boarding_passes
            conn.execute(
                text(insert_into_link(
                    "Link_ticket_flights_boarding_passes", "boarding_passes", "link_ticket_flights_boarding_passes_hash_key", "hub_ticket_flights_hash_key", 
                    "hub_boarding_passes_hash_key", "link_hash", "ticket_no_flight_id", "ticket_no_flight_id"
                )), 
                all_keys_after
            )
        
        elif operation == "update":
            # обновляем сателиты (новая строчка + в самой последней записи обновляем end_date), а затем линки

            for i in range(len(sat_tables)):
                conn.execute(
                    text(insert_into_sattelite(sat_tables[i], "boarding_passes", "hub_boarding_passes_hash_key", "ticket_no_flight_id", sat_columns[i])), 
                    all_keys_after
                )

                conn.execute(text(
                    update_query_in_update_operation(
                        sat_tables[i], 
                        "hub_boarding_passes_hash_key = MD5((:ticket_no_flight_id)::varchar)"
                    )), all_keys_after
                )

            # теперь добавим запись и изменим самую последнюю (end_date) в линк
            conn.execute(
                text(insert_into_link(
                    "Link_ticket_flights_boarding_passes", "boarding_passes", "link_ticket_flights_boarding_passes_hash_key", "hub_ticket_flights_hash_key", 
                    "hub_boarding_passes_hash_key", "link_hash", "ticket_no_flight_id", "ticket_no_flight_id"
                )), 
                all_keys_after
            )

            conn.execute(text(
                update_query_in_update_operation(
                    "Link_ticket_flights_boarding_passes", 
                    "link_ticket_flights_boarding_passes_hash_key = ((:link_hash)::varchar)"
                )), all_keys_after
            )
        
        elif operation == "delete":
            # обновляем в сателитах end_date
            for i in range(len(sat_tables)):
                conn.execute(text(
                    update_query_in_delete_operation(
                        sat_tables[i], 
                        "hub_boarding_passes_hash_key = MD5((:ticket_no_flight_id)::varchar)"
                    )), all_keys_before
                )

            # и обновим end_date в линке
            conn.execute(text(
                update_query_in_delete_operation(
                    "Link_ticket_flights_boarding_passes", 
                    "link_ticket_flights_boarding_passes_hash_key = ((:link_hash)::varchar)"
                )), all_keys_before
            )


def process_flights(operation, before, after, ts):
    with engine.begin() as conn:
        # все связанные с этой таблицей таблицы в dwh
        # Hub_flights
        # Sattelite_flights_flight_no
        # Sattelite_flights_scheduled_departure
        # Sattelite_flights_scheduled_arrival
        # Sattelite_flights_departure_airport
        # Sattelite_flights_arrival_airport
        # Sattelite_flights_status
        # Sattelite_flights_aircraft_code
        # Sattelite_flights_actual_departure
        # Sattelite_flights_actual_arrival
        # Link_flights_airport_data_departure
        # Link_flights_airport_data_arrival
        # Link_flights_aircrafts_data

        all_keys_after = None
        all_keys_before = None
        if operation == "insert" or operation == "update":
            all_keys_after = {
                "flight_id": after["flight_id"],
                "time": ts,
                "flight_no": after["flight_no"],
                "scheduled_departure": after["scheduled_departure"],
                "scheduled_arrival": after["scheduled_arrival"],
                "departure_airport": after["departure_airport"],
                "arrival_airport": after["arrival_airport"],
                "status": after["status"],
                "aircraft_code": after["aircraft_code"],
                "actual_departure": after["actual_departure"],
                "actual_arrival": after["actual_arrival"],
                "link_hash_flights_airport_departure": concat_hash([after["flight_id"], after["departure_airport"]]),
                "link_hash_flights_airport_arrival": concat_hash([after["flight_id"], after["arrival_airport"]]),
                "link_hash_flights_aircrafts": concat_hash([after["flight_id"], after["aircraft_code"]]),
            }
        elif operation == "delete":
            all_keys_before = {
                "flight_id": before["flight_id"],
                "time": ts,
                "flight_no": before["flight_no"],
                "scheduled_departure": before["scheduled_departure"],
                "scheduled_arrival": before["scheduled_arrival"],
                "departure_airport": before["departure_airport"],
                "arrival_airport": before["arrival_airport"],
                "status": before["status"],
                "aircraft_code": before["aircraft_code"],
                "actual_departure": before["actual_departure"],
                "actual_arrival": before["actual_arrival"],
                "link_hash_flights_airport_departure": concat_hash([before["flight_id"], before["departure_airport"]]),
                "link_hash_flights_airport_arrival": concat_hash([before["flight_id"], before["arrival_airport"]]),
                "link_hash_flights_aircrafts": concat_hash([before["flight_id"], before["aircraft_code"]]),
            }

        sat_tables = [
            "Sattelite_flights_flight_no", "Sattelite_flights_scheduled_departure", "Sattelite_flights_scheduled_arrival",
            "Sattelite_flights_departure_airport", "Sattelite_flights_arrival_airport", "Sattelite_flights_status",
            "Sattelite_flights_aircraft_code", "Sattelite_flights_actual_departure", "Sattelite_flights_actual_arrival"
        ]
        sat_columns = [
            "flight_no", "scheduled_departure", "scheduled_arrival",
            "departure_airport", "arrival_airport", "status",
            "aircraft_code", "actual_departure", "actual_arrival"
        ]

        if operation == "insert":
            # добавляем запись в хаб
            conn.execute(
                text(insert_into_hub("Hub_flights", "flights", "flight_id")), 
                all_keys_after
            )

            # атрибуты добавляем в сателиты
            for i in range(len(sat_tables)):
                conn.execute(
                    text(insert_into_sattelite(sat_tables[i], "flights", "hub_flights_hash_key", "flight_id", sat_columns[i])), 
                    all_keys_after
                )

            # теперь добавляем линк flights c airport_departure
            conn.execute(
                text(insert_into_link(
                    "Link_flights_airport_data_departure", "flights", "link_flights_airport_data_departure_hash_key", "hub_flights_hash_key", 
                    "hub_airports_data_hash_key", "link_hash_flights_airport_departure", "flight_id", "departure_airport"
                )), 
                all_keys_after
            )

            # теперь добавляем линк flights c airport_arrival
            conn.execute(
                text(insert_into_link(
                    "Link_flights_airport_data_arrival", "flights", "link_flights_airport_data_arrival_hash_key", "hub_flights_hash_key", 
                    "hub_airports_data_hash_key", "link_hash_flights_airport_arrival", "flight_id", "arrival_airport"
                )), 
                all_keys_after
            )

            # теперь добавляем линк flights c aircrafts
            conn.execute(
                text(insert_into_link(
                    "Link_flights_aircrafts_data", "flights", "link_flights_aircrafts_data_hash_key", "hub_flights_hash_key", 
                    "hub_aircrafts_data_hash_key", "link_hash_flights_aircrafts", "flight_id", "aircraft_code"
                )), 
                all_keys_after
            )

        elif operation == "update":
            # обновляем сателиты (новая строчка + в самой последней записи обновляем end_date), а затем линки
            
            for i in range(len(sat_tables)):
                conn.execute(
                    text(insert_into_sattelite(sat_tables[i], "flights", "hub_flights_hash_key", "flight_id", sat_columns[i])), 
                    all_keys_after
                )

                conn.execute(text(
                    update_query_in_update_operation(
                        sat_tables[i], 
                        "hub_flights_hash_key = MD5((:flight_id)::varchar)"
                    )), all_keys_after
                )

            # теперь добавим запись и изменим самую последнюю (end_date) в линках            
            conn.execute(
                text(insert_into_link(
                    "Link_flights_airport_data_departure", "flights", "link_flights_airport_data_departure_hash_key", "hub_flights_hash_key", 
                    "hub_airports_data_hash_key", "link_hash_flights_airport_departure", "flight_id", "departure_airport"
                )), 
                all_keys_after
            )

            conn.execute(text(
                update_query_in_update_operation(
                    "Link_flights_airport_data_departure", 
                    "link_flights_airport_data_departure_hash_key = ((:link_hash_flights_airport_departure)::varchar)"
                )), all_keys_after
            )

            conn.execute(
                text(insert_into_link(
                    "Link_flights_airport_data_arrival", "flights", "link_flights_airport_data_arrival_hash_key", "hub_flights_hash_key", 
                    "hub_airports_data_hash_key", "link_hash_flights_airport_arrival", "flight_id", "arrival_airport"
                )), 
                all_keys_after
            )

            conn.execute(text(
                update_query_in_update_operation(
                    "Link_flights_airport_data_arrival", 
                    "link_flights_airport_data_arrival_hash_key = ((:link_hash_flights_airport_arrival)::varchar)"
                )), all_keys_after
            )

            conn.execute(
                text(insert_into_link(
                    "Link_flights_aircrafts_data", "flights", "link_flights_aircrafts_data_hash_key", "hub_flights_hash_key", 
                    "hub_aircrafts_data_hash_key", "link_hash_flights_aircrafts", "flight_id", "aircraft_code"
                )), 
                all_keys_after
            )

            conn.execute(text(
                update_query_in_update_operation(
                    "Link_flights_aircrafts_data", 
                    "link_flights_aircrafts_data_hash_key = ((:link_hash_flights_aircrafts)::varchar)"
                )), all_keys_after
            )

        elif operation == "delete":
            # обновляем в сателитах end_date
            for i in range(len(sat_tables)):
                conn.execute(text(
                    update_query_in_delete_operation(
                        sat_tables[i], 
                        "hub_flights_hash_key = MD5((:flight_id)::varchar)"
                    )), all_keys_before
                )

            # и обновим end_date в линках
            conn.execute(text(
                update_query_in_delete_operation(
                    "Link_flights_airport_data_departure", 
                    "link_flights_airport_data_departure_hash_key = ((:link_hash_flights_airport_departure)::varchar)"
                )), all_keys_before
            )

            conn.execute(text(
                update_query_in_delete_operation(
                    "Link_flights_airport_data_arrival", 
                    "link_flights_airport_data_arrival_hash_key = ((:link_hash_flights_airport_arrival)::varchar)"
                )), all_keys_before
            )

            conn.execute(text(
                update_query_in_delete_operation(
                    "Link_flights_aircrafts_data", 
                    "link_flights_aircrafts_data_hash_key = ((:link_hash_flights_aircrafts)::varchar)"
                )), all_keys_before
            )


def process_ticket_flights(operation, before, after, ts):
    with engine.begin() as conn:
        # все связанные с этой таблицей таблицы в dwh
        # Hub_ticket_flights
        # Sattelite_ticket_flights_amount
        # Sattelite_ticket_flights_fare_conditions
        # Link_ticket_flights_tickets
        # Link_flights_ticket_flights

        all_keys_after = None
        all_keys_before = None
        if operation == "insert" or operation == "update":
            all_keys_after = {
                "ticket_no_flight_id": str(after["ticket_no"]) + "," + str(after["flight_id"]),
                "time": ts,
                "link_hash_ticket_flights_tickets": concat_hash([str(after["ticket_no"]) + "," + str(after["flight_id"]), after["ticket_no"]]),
                "ticket_no": after["ticket_no"],
                "link_hash_flights_ticket_flights": concat_hash([after["flight_id"], str(after["ticket_no"]) + "," + str(after["flight_id"])]),
                "flight_id": after["flight_id"],
                "amount": after["amount"],
                "fare_conditions": after["fare_conditions"],
            }
        elif operation == "delete":
            all_keys_before = {
                "ticket_no_flight_id": str(before["ticket_no"]) + "," + str(before["flight_id"]),
                "time": ts,
                "link_hash_ticket_flights_tickets": concat_hash([str(before["ticket_no"]) + "," + str(before["flight_id"]), before["ticket_no"]]),
                "ticket_no": before["ticket_no"],
                "link_hash_flights_ticket_flights": concat_hash([before["flight_id"], str(before["ticket_no"]) + "," + str(before["flight_id"])]),
                "flight_id": before["flight_id"],
                "amount": before["amount"],
                "fare_conditions": before["fare_conditions"],
            }
        
        sat_tables = ["Sattelite_ticket_flights_amount", "Sattelite_ticket_flights_fare_conditions"]
        sat_columns = ["amount", "fare_conditions"]

        if operation == "insert":
            # добавляем запись в хаб
            conn.execute(
                text(insert_into_hub("Hub_ticket_flights", "ticket_flights", "ticket_no_flight_id")), 
                all_keys_after
            )

            # атрибуты добавляем в сателиты
            for i in range(len(sat_tables)):
                conn.execute(
                    text(insert_into_sattelite(sat_tables[i], "ticket_flights", "hub_ticket_flights_hash_key", "ticket_no_flight_id", sat_columns[i])), 
                    all_keys_after
                )

            # теперь добавляем линк ticket_flights c tickets
            conn.execute(
                text(insert_into_link(
                    "Link_ticket_flights_tickets", "ticket_flights", "link_ticket_flights_tickets_hash_key", "hub_ticket_flights_hash_key", 
                    "hub_tickets_hash_key", "link_hash_ticket_flights_tickets", "ticket_no_flight_id", "ticket_no"
                )), 
                all_keys_after
            )

            # теперь добавляем линк ticket_flights c flights
            conn.execute(
                text(insert_into_link(
                    "Link_flights_ticket_flights", "ticket_flights", "link_flights_ticket_flights_hash_key", "hub_flights_hash_key", 
                    "hub_ticket_flights_hash_key", "link_hash_flights_ticket_flights", "flight_id", "ticket_no_flight_id"
                )), 
                all_keys_after
            )
        
        elif operation == "update":
            # обновляем сателиты (новая строчка + в самой последней записи обновляем end_date), а затем линки
            
            for i in range(len(sat_tables)):
                conn.execute(
                    text(insert_into_sattelite(sat_tables[i], "ticket_flights", "hub_ticket_flights_hash_key", "ticket_no_flight_id", sat_columns[i])), 
                    all_keys_after
                )

                conn.execute(text(
                    update_query_in_update_operation(
                        sat_tables[i], 
                        "hub_ticket_flights_hash_key = MD5((:ticket_no_flight_id)::varchar)" 
                    )), all_keys_after
                )
            
            # теперь добавим запись и изменим самую последнюю (end_date) в линках            
            conn.execute(
                text(insert_into_link(
                    "Link_ticket_flights_tickets", "ticket_flights", "link_ticket_flights_tickets_hash_key", "hub_ticket_flights_hash_key", 
                    "hub_tickets_hash_key", "link_hash_ticket_flights_tickets", "ticket_no_flight_id", "ticket_no"
                )), 
                all_keys_after
            )

            conn.execute(text(
                update_query_in_update_operation(
                    "Link_ticket_flights_tickets", 
                    "link_ticket_flights_tickets_hash_key = ((:link_hash_ticket_flights_tickets)::varchar)"
                )), all_keys_after
            )

            conn.execute(
                text(insert_into_link(
                    "Link_flights_ticket_flights", "ticket_flights", "link_flights_ticket_flights_hash_key", "hub_flights_hash_key", 
                    "hub_ticket_flights_hash_key", "link_hash_flights_ticket_flights", "flight_id", "ticket_no_flight_id"
                )), 
                all_keys_after
            )

            conn.execute(text(
                update_query_in_update_operation(
                    "Link_flights_ticket_flights", 
                    "link_flights_ticket_flights_hash_key = ((:link_hash_flights_ticket_flights)::varchar)"
                )), all_keys_after
            )

        elif operation == "delete":
            # обновляем в сателитах end_date
            for i in range(len(sat_tables)):
                conn.execute(text(
                    update_query_in_delete_operation(
                        sat_tables[i], 
                        "hub_ticket_flights_hash_key = MD5((:ticket_no_flight_id)::varchar)" 
                    )), all_keys_before
                )

            # и обновим end_date в линках
            conn.execute(text(
                update_query_in_delete_operation(
                    "Link_ticket_flights_tickets", 
                    "link_ticket_flights_tickets_hash_key = ((:link_hash_ticket_flights_tickets)::varchar)"
                )), all_keys_before
            )

            conn.execute(text(
                update_query_in_delete_operation(
                    "Link_flights_ticket_flights", 
                    "link_flights_ticket_flights_hash_key = ((:link_hash_flights_ticket_flights)::varchar)"
                )), all_keys_before
            )


def process_message(message):
    ts = int(message["ts_ms"] / 1000)
    before = message["before"]
    after = message["after"]

    operation = None
    if before is None and after is not None:
        operation = "insert"
    elif before is not None and after is not None:
        operation = "update"
    elif before is not None and after is None:
        operation = "delete"

    if message["source"]["table"] == "bookings":
        process_bookings(operation, before, after, ts)
    elif message["source"]["table"] == "tickets":
        process_tickets(operation, before, after, ts)
    elif message["source"]["table"] == "airports":
        process_airports(operation, before, after, ts)
    elif message["source"]["table"] == "aircrafts":
        process_aircrafts(operation, before, after, ts)
    elif message["source"]["table"] == "seats":
        process_seats(operation, before, after, ts)
    elif message["source"]["table"] == "flights":
        process_flights(operation, before, after, ts)
    elif message["source"]["table"] == "ticket_flights":
        process_ticket_flights(operation, before, after, ts)
    elif message["source"]["table"] == "boarding_passes":
        process_boarding_passes(operation, before, after, ts)


logging.info("I AM STARTING!")
for message in consumer:
    if message.value is not None and "payload" in message.value:
        logging.info(message.value["payload"])
        process_message(message.value["payload"])
