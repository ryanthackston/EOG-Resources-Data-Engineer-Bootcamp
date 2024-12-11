# book_reader_multiprocess.py
import multiprocessing
import glob
import time

def count_characters(file_path):
    """
    Reads a file and counts its characters.
    """
    with open(file_path, 'r', encoding='utf-8') as file:
        return len(file.read())

def main():
    """
    Main function to count characters in all files of the 'copies' folder using multiprocessing.
    """
    # Fetch all file paths from the 'copies' folder
    file_paths = glob.glob("copies/*")

    if not file_paths:
        print("No files found in the 'copies' folder.")
        return

    # Start timer
    start_time = time.time()

    # Use all available CPU cores for multiprocessing
    with multiprocessing.Pool(processes=multiprocessing.cpu_count()) as pool:
        character_counts = pool.map(count_characters, file_paths)

    # Calculate total characters and elapsed time
    total_characters = sum(character_counts)
    duration = time.time() - start_time

    # Print the results
    print(f"\nTotal characters in all files: {total_characters}")
    print(f"Processed {len(file_paths)} files in {duration:.4f} seconds.")

if __name__ == '__main__':
    main()
