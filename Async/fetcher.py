import aiohttp
import asyncio
import requests

def fetch_url_sync(url):
    response = requests.get(url)
    return response.text

async def fetch_url_async(session, url):
    async with session.get(url) as response:
        return await response.text()
    
async def fetch_all_async(urls):
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_url_async(session, url) for url in urls]
        results = await asyncio.gather(*tasks)
        return results