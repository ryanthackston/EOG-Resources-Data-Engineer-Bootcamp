import multiprocessing

# Define a simple function that calculates and prints the square of a number
def calculate_square(number):
    result = number * number 
    print(f'The square of {number} is {result}')

if __name__ == '__main__':
    print(f"CPU count: {multiprocessing.cpu_count()}")
    # Create a list of numbers
    numbers = list(range(1,30))

    # Create a list to keep all prcoesses
    processes = []

    # Create a process for each number in the list
    for number in numbers:
        process = multiprocessing.Process(target=calculate_square, args=(number,))
        processes.append(process)
        # Start the process
        process.start()
    
    # Ensure all processes have finished execution
    for process in processes:
        process.join()

    print("All processes have finished")



