
## How to Extract and Store Daily Gas Prices Using Web Scraping and Airflow

![](https://cdn-images-1.medium.com/max/4000/1*V9IxuqEGQLQhVMxmrwc0uw.png)

In the era of big data, the ability to extract, transform, and store data from the web into different storage systems like MongoDB, PostgreSQL, MinIO, and Elasticsearch is a crucial skill for developers and data scientists. The ability to then expose this data through an API for use in front-end applications makes this skill even more valuable. This article will demonstrate how to do this using Python, Airflow, and FastAPI.

We’ll be creating a system that scrapes the web for the day’s highest and lowest gas prices, stores this data in various databases, and then exposes this data through an API. The API can be utilized in a React application or any other front-end framework.

For this example, the data structure we’ll be using is:

    {
      "highest_price": {
        "price": 163.9,
        "station": "Esso Du Commerce / René Lévesque",
        "city": "Verdun ( Île des Soeurs )"
      },
      "lowest_price": {
        "price": 154.4,
        "station": "Costco 2999 Aut.440 Laval/Ave. Jacques-Bureau",
        "city": "Laval"
      }
    }

## Web Scraping

    from datetime import datetime
    from airflow import DAG
    from airflow.operators.python_operator import PythonOperator
    from airflow.utils.dates import days_ago
    from airflow.providers.mongo.hooks.mongo import MongoHook
    import requests
    from bs4 import BeautifulSoup
    from datetime import date
    
    # Constants
    DAG_ID = 'gas_prices_load_mongodb_dag'
    MONGO_CONN_ID = 'mongodb_default'  # Replace with your MongoDB connection ID
    
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
    
    def save_to_mongodb(**context):
        gas_prices = context['task_instance'].xcom_pull(task_ids='scrape_gas_prices_task')
    
        mongo_hook = MongoHook(conn_id=MONGO_CONN_ID)
        mongo_client = mongo_hook.get_conn()
        
        # Replace 'gas_prices_db' and 'gas_prices' with your database and collection names
        collection = mongo_client['gas_prices_db']['gas_prices']
    
        for price_info in gas_prices:
            doc = {
                'price': price_info[0],
                'station': price_info[1],
                'city': price_info[2],
                'time': price_info[3],
                'user': price_info[4],
                'date': datetime.strptime(price_info[5], '%Y-%m-%d'),
            }
            collection.insert_one(doc)
    
    default_args = {
        'owner': 'airflow',
        'start_date': days_ago(1),
        'retries': 0,
    }
    
    dag = DAG(
        DAG_ID,
        default_args=default_args,
        description='DAG to scrape gas prices and save to MongoDB',
        schedule_interval='@daily',
    )
    
    scrape_gas_prices_task = PythonOperator(
        task_id='scrape_gas_prices_task',
        python_callable=scrape_gas_prices,
        dag=dag,
    )
    
    save_to_mongodb_task = PythonOperator(
        task_id='save_to_mongodb_task',
        python_callable=save_to_mongodb,
        provide_context=True,
        dag=dag,
    )
    
    scrape_gas_prices_task >> save_to_mongodb_task

## API:

    from typing import Dict, Any
    from fastapi import FastAPI
    from pydantic import BaseModel, Field
    from pymongo import MongoClient
    from bson import ObjectId
    from datetime import datetime, timedelta
    from fastapi import FastAPI
    from fastapi.middleware.cors import CORSMiddleware
    
    class PyObjectId(ObjectId):
        @classmethod
        def __get_validators__(cls):
            yield cls.validate
    
        @classmethod
        def validate(cls, v):
            if not ObjectId.is_valid(v):
                raise ValueError('Invalid objectid')
            return ObjectId(v)
    
        @classmethod
        def __modify_schema__(cls, field_schema):
            field_schema.update(type='string')
    
    class DBModel(BaseModel):
        id: PyObjectId = Field(default_factory=PyObjectId, alias='_id')
    
        class Config:
            arbitrary_types_allowed = True
            json_encoders = {
                ObjectId: str
            }
            allow_population_by_field_name = True
    
    class GasPriceModel(BaseModel):
        price: float
        station: str
        city: str
    
    app = FastAPI()
    
    origins = [
        "http://localhost:3000",  # React app
        "http://localhost:8000",  # FastAPI server
    ]
    
    app.add_middleware(
        CORSMiddleware,
        allow_origins=origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    def get_db_connection():
        client = MongoClient(
            host="172.23.0.2",  # replace with your MongoDB host
            username="root",  # replace with your MongoDB username
            password="example"  # replace with your MongoDB password
        )
        db = client["gas_prices_db"]
        return db
    
    @app.get("/prices", response_model=Dict[str, GasPriceModel])
    def get_prices() -> Dict[str, Any]:
        db = get_db_connection()
    
        # Get today's date and the date for the next day
        today = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
        next_day = today + timedelta(days=1)
    
        # Execute query to find row with highest price for today
        highest_price_row = db.gas_prices.find_one({"date": {"$gte": today, "$lt": next_day}}, sort=[("price", -1)])
    
        # Execute query to find row with lowest price for today
        lowest_price_row = db.gas_prices.find_one({"date": {"$gte": today, "$lt": next_day}}, sort=[("price", 1)])
    
        return {"highest_price": GasPriceModel(**highest_price_row), "lowest_price": GasPriceModel(**lowest_price_row)}
    
    # If you want to run the server using python script, uncomment the below lines
    
    if __name__ == '__main__':
          import uvicorn
          uvicorn.run(app, host="0.0.0.0", port=8000)

## Front-End-React:

    import React, { useState, useEffect } from 'react';
    import axios from 'axios';
    import { FaMoneyBillAlt, FaBuilding, FaCity } from 'react-icons/fa';
    
    
    const HomePage = () => {
      const [data, setData] = useState({ highest_price: {}, lowest_price: {} });
    
      useEffect(() => {
        const fetchData = async () => {
          const result = await axios('http://localhost:8000/prices');
          setData(result.data);
        };
    
        fetchData();
      }, []);
    
      return (
        
        <div className="container">
          <div className="price-card">
            <h2>Highest Price</h2>
            <p><FaMoneyBillAlt /> Price: {data.highest_price.price} $</p>
            <p><FaBuilding /> Station: {data.highest_price.station}</p>
            <p><FaCity /> City: {data.highest_price.city}</p>
          </div>
          <div className="price-card">
            <h2>Lowest Price</h2>
            <p><FaMoneyBillAlt /> Price: {data.lowest_price.price} $</p>
            <p><FaBuilding /> Station: {data.lowest_price.station}</p>
            <p><FaCity /> City: {data.lowest_price.city}</p>
          </div>
        </div>
      );
    }
    
    export default HomePage;
