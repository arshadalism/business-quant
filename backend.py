from fastapi import FastAPI, HTTPException
from db import ticker_data_db, ticker_two_column, five_years_data
import uvicorn
import mysql.connector
from datetime import datetime, timedelta
import asyncio


app = FastAPI(title="Business Quant")


@app.get("/")
def read_root():
    return {"This is the Business quant page."}


@app.get("/ticker/{name}")   # This is the api to fetch the data with the ticker name
async def ticker_data(name: str):
    data = await ticker_data_db(name)
    return data


@app.get("/ticker_two_column/{name}")   # this is the api to fetch the ticker name . revenue and gp column data.
async def ticker_two_column_data(name: str):
    data = await ticker_two_column(name)
    return data


@app.get("/ticker_five_years_data/{name}")  # this is to fetch the data of the last 5 years and only contains the ticker name , date, revenue and gp column data.
async def ticker_five_years_data(name: str):
    result = await five_years_data(name)
    return result


if __name__ == '__main__':
    uvicorn.run("backend:app", reload=True)
