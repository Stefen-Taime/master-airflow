import os
import requests
from bs4 import BeautifulSoup
from datetime import date
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.utils.dates import days_ago
from airflow.hooks.postgres_hook import PostgresHook

# Constants
DAG_ID = 'gas_prices_load_postgres_dag'
POSTGRES_CONN_ID = 'postgres_id'
SQL_CREATE_TABLE = """
    CREATE TABLE IF NOT EXISTS gas_prices (
        price varchar(255),
        station varchar(255),
        city varchar(255),
        time varchar(255),
        "user" varchar(255),
        date date
    );
"""

def scrape_gas_prices():
    url = "https://www.essencemontreal.com/prices.php?l=f&prov=QC&city=Montreal"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3"
    }

    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.text, 'html.parser')

    prices = soup.find_all('td', {'class': ['greencell', 'redcell', 'pricecell']})
    stations = soup.find_all('td', {'class': 'stationcell'})
    cities = soup.find_all('td', {'class': 'citycell'})
    times_users = soup.find_all('td', {'class': 'usercell'})

    gas_prices = []

    for price, station, city, time_user in zip(prices, stations, cities, times_users):
        gas_station = " ".join(station.stripped_strings)
        gas_city = " ".join(city.stripped_strings)
        gas_price = " ".join(price.stripped_strings)
        gas_time_user = " ".join(time_user.stripped_strings)

        # Splitting gas_time_user into time and user
        gas_time, *gas_user = gas_time_user.split(maxsplit=1)
        gas_user = ' '.join(gas_user)

        # Get today's date
        today = date.today()

        # Add today's date to the gas price information
        gas_prices.append((gas_price, gas_station, gas_city, gas_time, gas_user, str(today)))

    return gas_prices

def save_to_postgres(**context):
    gas_prices = context['task_instance'].xcom_pull(task_ids='scrape_gas_prices_task')

    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

    for price_info in gas_prices:
        pg_hook.run('INSERT INTO gas_prices VALUES (%s, %s, %s, %s, %s, %s)', parameters=price_info)

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 0,
}

dag = DAG(
    DAG_ID,
    default_args=default_args,
    description='DAG to scrape gas prices and save to PostgreSQL',
    schedule_interval='@daily',
)

create_table_task = PostgresOperator(
    task_id='create_table_task',
    postgres_conn_id=POSTGRES_CONN_ID,
    sql=SQL_CREATE_TABLE,
    dag=dag,
)

scrape_gas_prices_task = PythonOperator(
    task_id='scrape_gas_prices_task',
    python_callable=scrape_gas_prices,
    dag=dag,
)

save_to_postgres_task = PythonOperator(
    task_id='save_to_postgres_task',
    python_callable=save_to_postgres,
    provide_context=True,
    dag=dag,
)

create_table_task >> scrape_gas_prices_task >> save_to_postgres_task
