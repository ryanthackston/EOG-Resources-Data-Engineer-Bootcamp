import time 
from fetcher import fetch_url_sync

def main():
    urls = [
        "https://example.com",
        "https://httpbin.org/get",
        "https://jsonplaceholder.typicode.com/todos/1"
    ]
    start_time = time.time()

    for url in urls:
        result = fetch_url_sync(url)
        print(f"Fetched {len(result)} characters from {url}")

    duration = time.time() - start_time
    print(f"Blocking execution took {duration} seconds")

if __name__ == "__main__":
    main()