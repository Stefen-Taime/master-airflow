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
