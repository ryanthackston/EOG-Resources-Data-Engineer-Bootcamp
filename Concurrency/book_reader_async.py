import asyncio
import os
import glob
import time

# Read file asynchronously using aiofiles
import aiofiles

async def read_file_async(file_path):
    """
    Asynchronously reads the content of a file and returns its character count, file path, and time taken.
    """
    start_time = time.time()
    try:
        async with aiofiles.open(file_path, mode='r', encoding='utf-8') as file:
            content = await file.read()
    except Exception as e:
        print(f"Error reading file {file_path}: {e}")
        return 0, file_path, 0.0

    end_time = time.time()
    return len(content), file_path, end_time - start_time

async def count_characters_in_folder(folder_path):
    """
    Asynchronously counts the total number of characters in all files in the folder.
    """
    # Fetch all file paths in the folder
    file_paths = glob.glob(os.path.join(folder_path, '*'))

    if not file_paths:
        print("No files found in the folder.")
        return

    start_time = time.time()

    # Create and run tasks for reading all files
    tasks = [read_file_async(file_path) for file_path in file_paths]
    results = await asyncio.gather(*tasks)

    duration = time.time() - start_time

    # Aggregate total characters from all results
    total_characters = sum(length for length, _, _ in results)

    # Print results for each file
    for length, file_path, file_duration in results:
        print(f"Read {length} characters from {file_path} in {file_duration:.4f} seconds.")

    # Print the total number of characters
    print(f"\nTotal characters in all files: {total_characters}")
    print(f"Async processing took {duration:.4f} seconds.")

def main():
    """
    Main entry point for the script.
    """
    folder_path = "Books"
    asyncio.run(count_characters_in_folder(folder_path))

if __name__ == '__main__':
    main()
