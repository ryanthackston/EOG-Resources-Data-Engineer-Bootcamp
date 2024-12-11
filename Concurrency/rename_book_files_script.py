import os
import shutil
import string

def rename_and_copy_all_books(source_folder, destination_folder):
    # Ensure the source folder exists
    if not os.path.exists(source_folder):
        print(f"The source folder '{source_folder}' does not exist.")
        return

    # Create the destination folder if it doesn't exist
    if not os.path.exists(destination_folder):
        os.makedirs(destination_folder)

    # Get a sorted list of files in the source folder
    books = sorted(os.listdir(source_folder))

    # Check if there are any files in the source folder
    if not books:
        print("The source folder is empty.")
        return

    # Process each book in the source folder
    for index, book in enumerate(books, start=1):
        original_file_path = os.path.join(source_folder, book)

        # Skip directories, only process files
        if os.path.isdir(original_file_path):
            print(f"Skipping directory '{book}'.")
            continue

        # Generate the base file name for the current book
        base_file_name = f"T8_FILE_{index}"

        # Create 26 copies of the file, each with a unique name
        for letter in string.ascii_uppercase:
            new_file_name = f"{base_file_name}_{letter}{os.path.splitext(book)[1]}"
            new_file_path = os.path.join(destination_folder, new_file_name)

            # Copy the file and rename
            shutil.copy2(original_file_path, new_file_path)

        print(f"The file '{book}' has been copied and renamed 26 times.")

    print(f"All files have been copied to '{destination_folder}' successfully.")

# Set the paths to the source and destination folders
source_folder_path = "Books"
destination_folder_path = "Books2"

# Call the function to rename and copy all books
rename_and_copy_all_books(source_folder_path, destination_folder_path)

