# ----------------- ИМПОРТЫ ----------------

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

# ---------- ПЕРЕМЕННЫЕ/КОНСТАНТЫ ----------

sql_script_1 = '''
    CREATE SCHEMA IF NOT EXISTS presentation;
'''

# запрос, который должен быть по заданию
# sql_script_2 = '''
#     CREATE TABLE IF NOT EXISTS presentation.passenger_traffic_airports (
#         created_at timestamp,
#         flight_date char(13),
#         airport_code char(3),
#         linked_airport_code char(3),
#         flights_in integer,
#         flights_out integer,
#         passengers_in integer,
#         passengers_out integer
#     );

#     with joined_table as (
#         select distinct 
#             hub_flights.flight_id,
#             sattelite_flights_actual_arrival.actual_arrival,
#             sattelite_flights_departure_airport.departure_airport,
#             sattelite_flights_arrival_airport.arrival_airport,
#             sattelite_boarding_passes_boarding_no.boarding_no
#         from
#             dwh_detailed.hub_flights hub_flights
            
#             inner join dwh_detailed.sattelite_flights_actual_arrival sattelite_flights_actual_arrival
#                 on hub_flights.hash_key = sattelite_flights_actual_arrival.hub_flights_hash_key
            
#             inner join dwh_detailed.sattelite_flights_departure_airport sattelite_flights_departure_airport
#                 on hub_flights.hash_key = sattelite_flights_departure_airport.hub_flights_hash_key
            
#             inner join dwh_detailed.sattelite_flights_arrival_airport sattelite_flights_arrival_airport
#                 on hub_flights.hash_key = sattelite_flights_arrival_airport.hub_flights_hash_key

#             inner join dwh_detailed.link_flights_ticket_flights link_flights_ticket_flights
#                 on hub_flights.hash_key = link_flights_ticket_flights.hub_flights_hash_key

#             inner join dwh_detailed.hub_ticket_flights hub_ticket_flights
#                 on link_flights_ticket_flights.hub_ticket_flights_hash_key = hub_ticket_flights.hash_key
            
#             inner join dwh_detailed.link_ticket_flights_boarding_passes link_ticket_flights_boarding_passes
#                 on hub_ticket_flights.hash_key = link_ticket_flights_boarding_passes.hub_ticket_flights_hash_key
            
#             inner join dwh_detailed.hub_boarding_passes hub_boarding_passes
#                 on link_ticket_flights_boarding_passes.hub_boarding_passes_hash_key = hub_boarding_passes.hash_key
            
#             inner join dwh_detailed.sattelite_boarding_passes_boarding_no sattelite_boarding_passes_boarding_no
#                 on hub_boarding_passes.hash_key = sattelite_boarding_passes_boarding_no.hub_boarding_passes_hash_key
#         where 
#             sattelite_flights_actual_arrival.load_end_date is null
#             and sattelite_flights_departure_airport.load_end_date is null
#             and sattelite_flights_arrival_airport.load_end_date is null
#             and link_flights_ticket_flights.load_end_date is null
#             and link_ticket_flights_boarding_passes.load_end_date is null
#             and sattelite_boarding_passes_boarding_no.load_end_date is null
#         ),
#         stat_in_day as (
#             select 
#                 departure_airport, arrival_airport, cast(actual_arrival::date as char(13)) as flight_date,
#                 count(*) as passengers_count,
#                 count(distinct flight_id) as flights_count
#             from 
#                 joined_table
#             where 
#                 actual_arrival::date = TO_TIMESTAMP('{calc_date}', 'DD-MM-YYYY HH24:MI:SS')::date - 1 
#             group by 
#                 departure_airport, arrival_airport, actual_arrival::date
#         ),
#         pairs_airports as (
#             select
#                 t1.airport_code as airport_code, 
#                 t2.airport_code as linked_airport_code
#             from 
#                 dwh_detailed.hub_airports_data t1
#                 join dwh_detailed.hub_airports_data t2 
#                 on t1.airport_code < t2.airport_code
#         )
    
#     delete from presentation.passenger_traffic_airports
#     where
#         created_at::date = TO_TIMESTAMP('{calc_date}', 'DD-MM-YYYY HH24:MI:SS')::date;
    
#     insert into presentation.passenger_traffic_airports (
#         created_at,
#         flight_date,
#         airport_code,
#         linked_airport_code,
#         flights_in,
#         flights_out,
#         passengers_in,
#         passengers_out
#     )
#     select 
#         TO_TIMESTAMP('{calc_date}', 'DD-MM-YYYY HH24:MI:SS') AS created_at,
#         coalesce(stat_1.flight_date, stat_2.flight_date) as flight_date,
#         pa.airport_code,
#         pa.linked_airport_code,
#         coalesce(stat_2.flights_count, 0) as flights_in,
#         coalesce(stat_1.flights_count, 0) as flights_out,
#         coalesce(stat_2.passengers_count, 0) as passengers_in,
#         coalesce(stat_1.passengers_count, 0) as passengers_out
#     from 
#         pairs_airports as pa
#         left join stat_in_day as stat_1 
#         on pa.airport_code = stat_1.departure_airport and pa.linked_airport_code = stat_1.arrival_airport
#         left join stat_in_day as stat_2 
#         on pa.airport_code = stat_2.arrival_airport and pa.linked_airport_code = stat_2.departure_airport
#     where 
#         stat_1.flight_date is not null
#         or stat_2.flight_date is not null;
# '''


# аналог запроса по заданию, но удобный для дебага/тестов, т.к. пересчитывает для всех дат, а не только за вчера
sql_script_2 = '''
    DROP TABLE IF EXISTS presentation.passenger_traffic_airports;

    CREATE TABLE IF NOT EXISTS presentation.passenger_traffic_airports (
        created_at timestamp,
        flight_date char(13),
        airport_code char(3),
        linked_airport_code char(3),
        flights_in integer,
        flights_out integer,
        passengers_in integer,
        passengers_out integer
    );

    with joined_table as (
        select distinct 
            hub_flights.flight_id,
            sattelite_flights_actual_arrival.actual_arrival,
            sattelite_flights_departure_airport.departure_airport,
            sattelite_flights_arrival_airport.arrival_airport,
            sattelite_boarding_passes_boarding_no.boarding_no
        from
            dwh_detailed.hub_flights hub_flights
            
            inner join dwh_detailed.sattelite_flights_actual_arrival sattelite_flights_actual_arrival
                on hub_flights.hash_key = sattelite_flights_actual_arrival.hub_flights_hash_key
            
            inner join dwh_detailed.sattelite_flights_departure_airport sattelite_flights_departure_airport
                on hub_flights.hash_key = sattelite_flights_departure_airport.hub_flights_hash_key
            
            inner join dwh_detailed.sattelite_flights_arrival_airport sattelite_flights_arrival_airport
                on hub_flights.hash_key = sattelite_flights_arrival_airport.hub_flights_hash_key

            inner join dwh_detailed.link_flights_ticket_flights link_flights_ticket_flights
                on hub_flights.hash_key = link_flights_ticket_flights.hub_flights_hash_key

            inner join dwh_detailed.hub_ticket_flights hub_ticket_flights
                on link_flights_ticket_flights.hub_ticket_flights_hash_key = hub_ticket_flights.hash_key
            
            inner join dwh_detailed.link_ticket_flights_boarding_passes link_ticket_flights_boarding_passes
                on hub_ticket_flights.hash_key = link_ticket_flights_boarding_passes.hub_ticket_flights_hash_key
            
            inner join dwh_detailed.hub_boarding_passes hub_boarding_passes
                on link_ticket_flights_boarding_passes.hub_boarding_passes_hash_key = hub_boarding_passes.hash_key
            
            inner join dwh_detailed.sattelite_boarding_passes_boarding_no sattelite_boarding_passes_boarding_no
                on hub_boarding_passes.hash_key = sattelite_boarding_passes_boarding_no.hub_boarding_passes_hash_key
        where 
            sattelite_flights_actual_arrival.load_end_date is null
            and sattelite_flights_departure_airport.load_end_date is null
            and sattelite_flights_arrival_airport.load_end_date is null
            and link_flights_ticket_flights.load_end_date is null
            and link_ticket_flights_boarding_passes.load_end_date is null
            and sattelite_boarding_passes_boarding_no.load_end_date is null
        ),
        stat_in_day as (
            select 
                departure_airport, arrival_airport, cast(actual_arrival::date as char(13)) as flight_date,
                count(*) as passengers_count,
                count(distinct flight_id) as flights_count
            from 
                joined_table
            group by 
                departure_airport, arrival_airport, actual_arrival::date
        ),
        pairs_airports as (
            select
                t1.airport_code as airport_code, 
                t2.airport_code as linked_airport_code
            from 
                dwh_detailed.hub_airports_data t1
                join dwh_detailed.hub_airports_data t2 
                on t1.airport_code < t2.airport_code
        )
    
    insert into presentation.passenger_traffic_airports (
        created_at,
        flight_date,
        airport_code,
        linked_airport_code,
        flights_in,
        flights_out,
        passengers_in,
        passengers_out
    )
    select 
        TO_TIMESTAMP('{calc_date}', 'DD-MM-YYYY HH24:MI:SS') AS created_at,
        coalesce(stat_1.flight_date, stat_2.flight_date) as flight_date,
        pa.airport_code,
        pa.linked_airport_code,
        coalesce(stat_2.flights_count, 0) as flights_in,
        coalesce(stat_1.flights_count, 0) as flights_out,
        coalesce(stat_2.passengers_count, 0) as passengers_in,
        coalesce(stat_1.passengers_count, 0) as passengers_out
    from 
        pairs_airports as pa
        left join stat_in_day as stat_1 
        on pa.airport_code = stat_1.departure_airport and pa.linked_airport_code = stat_1.arrival_airport
        left join stat_in_day as stat_2 
        on pa.airport_code = stat_2.arrival_airport and pa.linked_airport_code = stat_2.departure_airport
    where 
        stat_1.flight_date is not null
        or stat_2.flight_date is not null;
'''

DEFAULT_ARGS = {
    'owner': 'student',
    'depends_on_past': False,
    'start_date': datetime(2023, 11, 6),
    'email': None,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=120)
}

# ------------------- КОД ------------------

with DAG("passenger_traffic_airports",
         default_args=DEFAULT_ARGS,
         catchup=False,
         schedule_interval="@daily",
         max_active_runs=1,
         concurrency=1) as dag:

    task1 = PostgresOperator(
        task_id="create_schema",
        postgres_conn_id="postgres_dwh",
        sql=sql_script_1,
    )

    task2 = PostgresOperator(
        task_id="perform_etl",
        postgres_conn_id="postgres_dwh",
        sql=sql_script_2.format(calc_date="{{ execution_date.strftime('%d-%m-%Y %H:%M:%S') }}"),
    )

    task1 >> task2
