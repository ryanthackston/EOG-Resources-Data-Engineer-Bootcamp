from fastapi import FastAPI, HTTPException
import oracledb
import asyncio
import os
from dotenv import load_dotenv

load_dotenv()


app =  FastAPI()

async def create_pool():
    pool = await oracledb.create_pool(
        user = "your_user",
        password = "your_password",
        dsn = "your_dsn",
        min=2,
        max=10,
        increment=1
    )
    return pool

pool = asyncio.run(create_pool())

async def fetch_data(query: str):
    async with pool.acquire() as connection:
        async with connection.cursor() as cursor:
            await cursor.execute(query)
            result = await cursor.fetchall()
            return result
        
@app.get("/data")
async def get_data():
    try:
        query = "SELECT * FROM your_table"
        result = await fetch_data(query)
        return {"data": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
