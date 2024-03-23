import mysql.connector as sqltor
import os
from dotenv import load_dotenv
import asyncio
import aiomysql

load_dotenv()


mycon = sqltor.connect(host="localhost", user="root", passwd=os.getenv("PASSWORD"), database="financial_data")
cursor = mycon.cursor(dictionary=True)


async def ticker_data_db(name: str):
    cursor.execute("Select * from business_data WHERE ticker=%s", (name,))
    data = cursor.fetchall()
    if not data:
        raise HTTPException(status_code=404, detail="Ticker not found.")
    return data


async def ticker_two_column(name: str):
    cursor.execute("SELECT ticker, revenue, gp FROM business_data WHERE ticker=%s", (name,))
    data = cursor.fetchall()
    if not data:
        raise HTTPException(status_code=404, detail="Ticker not found.")
    return data


async def five_years_data(name: str):
    query = "SELECT ticker, date, revenue, gp FROM business_data WHERE ticker = %s AND date BETWEEN DATE_SUB(CURDATE(), INTERVAL 5 YEAR) AND CURDATE()"
    cursor.execute(query, (name,))
    result = cursor.fetchall()
    if not result:
        raise HTTPException(status_code=404, detail="Ticker not found")
    return result


if __name__ == '__main__':
    pass
