import multiprocessing
import os

def factorial(n):
    if n == 0 or n == 1:
        return 1
    else:
        return n * factorial(n - 1)
    
def worker(number):
    result = factorial(number)
    print(f'Factorial of {number} is {result}')

if __name__ == '__main__':
    print(f"CPU count: {multiprocessing.cpu_count()}")
    numbers = list(range(1,100))

    with multiprocessing.Pool(processes=os.cpu_count()) as pool:
        pool.map(worker, numbers)

    print("All processes have finished.")