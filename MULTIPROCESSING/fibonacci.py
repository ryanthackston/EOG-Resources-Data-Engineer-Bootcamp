import multiprocessing

def fibonacci(n, a=0, b=1):
    if n == 0:
        return a
    if n == 1:
        return b
    return fibonacci(n - 1, b, a + b)
    
if __name__ == '__main__':
    numbers = list(range(30,36))

    # Create the pool of worker processes
    with multiprocessing.Pool(processes=multiprocessing.cpu_count()) as pool:
        results = pool.map(fibonacci, numbers)

    # Print the results
    for number, result in zip(numbers, results):
        print(f'Fibonacci of {number} is {result}')
