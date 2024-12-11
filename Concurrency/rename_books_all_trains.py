import os
import shutil

def create_copies(input_folder, output_folder):
    # Ensure the output folder exists
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    # Get a list of files in the input folder
    files = [f for f in os.listdir(input_folder) if os.path.isfile(os.path.join(input_folder, f))]
    
    copy_number = 1  # Global copy number that doesn't reset

    for file_number, file_name in enumerate(files, start=1):
        input_file_path = os.path.join(input_folder, file_name)
        
        # Loop through prefixes T1 to T9
        for prefix_index in range(1, 10):
            prefix = f"T{prefix_index}"
            
            # Create 26 copies for each prefix
            for letter_index, letter in enumerate('ABCDEFGHIJKLMNOPQRSTUVWXYZ', start=1):
                copy_file_name = f"{prefix}_FILE_{file_number}_{letter}{os.path.splitext(file_name)[1]}"
                output_file_path = os.path.join(output_folder, copy_file_name)
                
                # Copy the file
                shutil.copy(input_file_path, output_file_path)
                
                copy_number += 1  # Increment the global copy number

if __name__ == "__main__":
    books_folder = "Books"  # Path to the books folder
    copies_folder = "copies"  # Path to the new copies folder
    
    create_copies(books_folder, copies_folder)