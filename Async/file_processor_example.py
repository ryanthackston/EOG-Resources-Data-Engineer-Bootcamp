import asyncio

def read_file_sync(file_path):
    with open(file_path, 'r') as file:
        return file.read()
    
async def read_file_async(file_path):
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, read_file_sync, file_path)

async def read_all_files_async(file_paths):
    tasks = [read_file_async(file_path) for file_path in file_paths]
    results = await asyncio.gather(*tasks)
    return results
