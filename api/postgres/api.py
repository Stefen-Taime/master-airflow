from typing import List
from flask import Flask
import psycopg2

app = Flask(__name__)

def get_db_connection():
    conn = psycopg2.connect(
        host="172.23.0.8",
        database="postgres",
        user="airflow",
        password="airflow"
    )
    return conn

@app.route("/prices")
def get_prices():
    conn = get_db_connection()
    cursor = conn.cursor()

    # Execute query to find row with highest price
    cursor.execute("SELECT * FROM gas_prices ORDER BY price DESC LIMIT 1")
    highest_price_row = cursor.fetchone()

    # Execute query to find row with lowest price
    cursor.execute("SELECT * FROM gas_prices ORDER BY price ASC LIMIT 1")
    lowest_price_row = cursor.fetchone()

    # Close cursor and connection
    cursor.close()
    conn.close()

    return {"highest_price": highest_price_row, "lowest_price": lowest_price_row}

if __name__ == '__main__':
    app.run()
