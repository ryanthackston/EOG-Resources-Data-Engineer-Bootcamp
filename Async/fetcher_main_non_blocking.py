import asyncio
import time 
import requests
from fetcher import fetch_all_async

async def main():
    urls = [
        "https://example.com",
        "https://httpbin.org/get",
        "https://jsonplaceholder.typicode.com/todos/1"
    ]
    start_time = time.time()

    results = await fetch_all_async(urls)
    for i, result in enumerate(results):
        print(f"Fetched {len(result)} characters from {urls[i]}")

    duration = time.time() - start_time
    print(f"Non-blocking execution took {duration:.2f} seconds")

if __name__ == "__main__":
    asyncio.run(main())