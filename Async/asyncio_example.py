import asyncio
import aiohttp

async def fetch_data():
    print("Start fetching data....")
    await asyncio.sleep(2)

    print("Data fetched")
    return "sample Data"

async def main():
    print("Main function start")

    data = await fetch_data()
    print(f"Received data: {data}")

    print("Main function end")

asyncio.run(main())


async def fetch_url(session, url):
    async with session.get(url) as response:
        return await response.text()  # Once response is done, it will return the the object text. Each response may take different amounts of time....
    
    
async def main():
    urls = [
        'http://example.com',
        'http://example.org',
        'http://example.net'
    ]

    async with aiohttp.ClientSession() as session:
         tasks = [fetch_url(session, url) for url in urls]
         responses = await asyncio.gether(*tasks)
         for response in responses:
             print(response)

asyncio.run(main())