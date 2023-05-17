from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime

default_args = {
    'start_date': datetime(2023, 5, 16),
}

with DAG('create_postgres_table',
         default_args=default_args,
         schedule_interval=None) as dag:

    create_table = PostgresOperator(
        task_id='create_table',
        postgres_conn_id='postgres_id',  # replace with your conn_id
        sql="""
        CREATE TABLE IF NOT EXISTS employee (
            id SERIAL PRIMARY KEY,
            first_name VARCHAR(40) NOT NULL,
            last_name VARCHAR(40) NOT NULL,
            birth_date DATE NOT NULL,
            joined_date DATE NOT NULL,
            department VARCHAR(40)
        );
        """
    )
