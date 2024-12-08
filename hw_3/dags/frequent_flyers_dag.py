# ----------------- ИМПОРТЫ ----------------

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

# ---------- ПЕРЕМЕННЫЕ/КОНСТАНТЫ ----------

sql_script_1 = '''
    CREATE SCHEMA IF NOT EXISTS presentation;
'''

sql_script_2 = '''
    DROP TABLE IF EXISTS presentation.frequent_flyers;
'''

sql_script_3 = '''
    CREATE TABLE presentation.frequent_flyers (
        created_at timestamp,
        passenger_id varchar(20),
        passenger_name text,
        flights_number integer,
        purchase_sum integer,
        popular_departure char(3),
        popular_arrival char(3),
        customer_group VARCHAR(10)
    );

    WITH passenger_table AS (
        SELECT
            sattelite_passenger_id.passenger_id,
            sattelite_passenger_name.passenger_name,
            sattelite_amount.amount,
            sattelite_arrival_airport.arrival_airport,
            sattelite_departure_airport.departure_airport
        FROM
            dwh_detailed.hub_tickets hub_tickets
    
            INNER JOIN dwh_detailed.sattelite_tickets_passenger_id sattelite_passenger_id
                ON  hub_tickets.hash_key = sattelite_passenger_id.hub_tickets_hash_key
    
            INNER JOIN dwh_detailed.sattelite_tickets_passenger_name sattelite_passenger_name
                ON  hub_tickets.hash_key = sattelite_passenger_name.hub_tickets_hash_key
    
            INNER JOIN dwh_detailed.link_ticket_flights_tickets link_ticket_flights_tickets
                ON link_ticket_flights_tickets.hub_tickets_hash_key = hub_tickets.hash_key
    
            INNER JOIN dwh_detailed.hub_ticket_flights hub_ticket_flights
                ON link_ticket_flights_tickets.hub_ticket_flights_hash_key = hub_ticket_flights.hash_key
    
            INNER JOIN dwh_detailed.sattelite_ticket_flights_amount sattelite_amount
                ON hub_ticket_flights.hash_key = sattelite_amount.hub_ticket_flights_hash_key
    
            INNER JOIN dwh_detailed.link_flights_ticket_flights link_flights_ticket_flights
                ON link_flights_ticket_flights.hub_ticket_flights_hash_key = hub_ticket_flights.hash_key
    
            INNER JOIN dwh_detailed.hub_flights hub_flights
                ON link_flights_ticket_flights.hub_flights_hash_key = hub_flights.hash_key
    
            INNER JOIN dwh_detailed.sattelite_flights_arrival_airport sattelite_arrival_airport
                ON hub_flights.hash_key = sattelite_arrival_airport.hub_flights_hash_key
    
            INNER JOIN dwh_detailed.sattelite_flights_departure_airport sattelite_departure_airport
                ON hub_flights.hash_key = sattelite_departure_airport.hub_flights_hash_key
        WHERE
            sattelite_passenger_id.load_end_date is null
            and sattelite_passenger_name.load_end_date is null
            and link_ticket_flights_tickets.load_end_date is null
            and sattelite_amount.load_end_date is null
            and link_flights_ticket_flights.load_end_date is null
            and sattelite_arrival_airport.load_end_date is null
            and sattelite_departure_airport.load_end_date is null
        ),
        passenger_stats AS (
            SELECT
                passenger_id,
                passenger_name,
                COUNT(*) AS flights_number,
                SUM(amount) AS purchase_sum
            FROM
                passenger_table
            GROUP BY
                passenger_id, passenger_name
        ),
        departure_stats AS (
            SELECT
                passenger_id,
                departure_airport,
                COUNT(*) AS departure_count,
                ROW_NUMBER() OVER (PARTITION BY passenger_id ORDER BY COUNT(*) DESC, departure_airport) AS departure_rank
            FROM
                passenger_table
            GROUP BY
                passenger_id, departure_airport
        ),
        arrival_stats AS (
            SELECT
                passenger_id,
                arrival_airport,
                COUNT(*) AS arrival_count,
                ROW_NUMBER() OVER (PARTITION BY passenger_id ORDER BY COUNT(*) DESC, arrival_airport) AS arrival_rank
            FROM
                passenger_table
            GROUP BY
                passenger_id, arrival_airport
        ),
        popular_departure AS (
            SELECT
                passenger_id,
                departure_airport AS popular_departure
            FROM
                departure_stats
            WHERE
                departure_rank = 1
        ),
        popular_arrival AS (
            SELECT
                passenger_id,
                arrival_airport AS popular_arrival
            FROM
                arrival_stats
            WHERE
                arrival_rank = 1
        ),
        gmv_ranking AS (
            SELECT
                passenger_id,
                purchase_sum,
                PERCENT_RANK() OVER (ORDER BY purchase_sum DESC) AS gmv_percentile
            FROM
                passenger_stats
        ),
        customer_grouping AS (
            SELECT
                passenger_id,
                CASE
                    WHEN gmv_percentile <= 0.05 THEN '5'
                    WHEN gmv_percentile <= 0.1 THEN '10'
                    WHEN gmv_percentile <= 0.25 THEN '25'
                    WHEN gmv_percentile <= 0.5 THEN '50'
                    ELSE '50+'
                END AS customer_group
            FROM
                gmv_ranking
        )

    INSERT INTO presentation.frequent_flyers (
        created_at,
        passenger_id,
        passenger_name,
        flights_number,
        purchase_sum,
        popular_departure,
        popular_arrival,
        customer_group
    )
    SELECT
        TO_TIMESTAMP('{calc_date}', 'DD-MM-YYYY HH24:MI:SS') AS created_at,
        ps.passenger_id,
        ps.passenger_name,
        ps.flights_number,
        ps.purchase_sum,
        pd.popular_departure,
        pa.popular_arrival,
        cg.customer_group
    FROM
        passenger_stats ps
    LEFT JOIN
        popular_departure pd ON ps.passenger_id = pd.passenger_id
    LEFT JOIN
        popular_arrival pa ON ps.passenger_id = pa.passenger_id
    LEFT JOIN
        customer_grouping cg ON ps.passenger_id = cg.passenger_id;
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

with DAG("frequent_flyers",
         default_args=DEFAULT_ARGS,
         catchup=False,
         schedule_interval="@daily",
         max_active_runs=1,
         concurrency=1) as dag:

    # 1 - Create schema
    task1 = PostgresOperator(
        task_id="create_schema",
        postgres_conn_id="postgres_dwh",
        sql=sql_script_1,
    )

    # 2 - drop table
    task2 = PostgresOperator(
        task_id="drop_table",
        postgres_conn_id="postgres_dwh",
        sql=sql_script_2,
    )

    # 3 - create table
    task3 = PostgresOperator(
        task_id="perform_etl",
        postgres_conn_id="postgres_dwh",
        sql=sql_script_3.format(calc_date="{{ execution_date.strftime('%d-%m-%Y %H:%M:%S') }}"),
    )

    task1 >> task2 >> task3
