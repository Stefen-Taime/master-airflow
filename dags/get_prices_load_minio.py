from datetime import datetime
from elasticsearch import Elasticsearch
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from airflow.providers.elasticsearch.hooks.elasticsearch import ElasticsearchHook
import requests
from bs4 import BeautifulSoup
from datetime import date
from minio import Minio
from minio.error import S3Error
import json
import io

# Constants
DAG_ID = 'gas_prices_load_minio_dag'
ELASTIC_CONN_ID = 'elasticsearch_default'

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

def save_to_minio(**context):
    gas_prices = context['task_instance'].xcom_pull(task_ids='scrape_gas_prices_task')

    # Setup MinIO client
    minioClient = Minio('minio',
                        access_key='minio',
                        secret_key='minio123',
                        secure=False)

    bucket_name = 'gas_prices'

    # Make a new bucket if it doesn't exist
    if not minioClient.bucket_exists(bucket_name):
        minioClient.make_bucket(bucket_name)

    for price_info in gas_prices:
        doc = {
            'price': price_info[0],
            'station': price_info[1],
            'city': price_info[2],
            'time': price_info[3],
            'user': price_info[4],
            'date': price_info[5],
        }
        json_doc = json.dumps(doc)

        # Save the data to MinIO
        minioClient.put_object(bucket_name, doc['date'] + doc['station'], io.BytesIO(json_doc.encode()), len(json_doc), content_type='application/json')

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 0,
}

dag = DAG(
    DAG_ID,
    default_args=default_args,
    description='DAG to scrape gas prices and save to Minio',
    schedule_interval='@daily',
)

scrape_gas_prices_task = PythonOperator(
    task_id='scrape_gas_prices_task',
    python_callable=scrape_gas_prices,
    dag=dag,
)

save_to_minio_task = PythonOperator(
    task_id='save_to_minio',
    python_callable=save_to_minio,
    provide_context=True,
    dag=dag,
)

scrape_gas_prices_task >> save_to_minio_task
