import asyncio
import time
from file_processor_example import read_file_sync, read_all_files_async

async def main():
    file_paths = [
        "sample_data/file1.txt",
        "sample_data/file2.txt",
        "sample_data/file3.txt"
    ]
    start_time = time.time()

    # for file_path in file_paths:
    #     content = read_file_sync(file_path)
    #     print(f"Read {len(content)} characters from {file_path}")

    contents = await read_all_files_async(file_paths)
    for i, content in enumerate(contents):
        print(f"Read {len(content)} characters from {file_paths[i]}")

    duration = time.time() - start_time
    print(f"Non-blocking execution took {duration:.2f} seconds")

if __name__ == "__main__":
    asyncio.run(main())
