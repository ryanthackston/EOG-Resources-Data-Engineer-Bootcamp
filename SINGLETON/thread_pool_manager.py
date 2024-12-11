# Import the threading module to create and manage threads in Python.
import threading

# Define a class named `ThreadPool` that manages a pool of threads using the singleton pattern.
class ThreadPool:
    # A class variable `_instance` to hold the single instance of the `ThreadPool` class.
    _instance = None

    # Override the __new__ method to control the creation of a single instance (singleton pattern).
    def __new__(cls):
        # Check if `_instance` is None, meaning no instance of `ThreadPool` exists yet.
        if cls._instance is None:
            # If no instance exists, create one by calling the parent class's __new__ method.
            cls._instance = super(ThreadPool, cls).__new__(cls)
            # Initialize an empty list `_pool` to store threads within the singleton instance.
            cls._instance._pool = []  # Initialize thread pool
        # Return the single instance of the class.
        return cls._instance

    # Method to add a new task to the thread pool.
    def add_task(self, task):
        # Create a new thread for the given `task`, where `task` is a callable (e.g., a function or lambda).
        thread = threading.Thread(target=task)
        # Add the new thread to the `_pool` list to keep track of it.
        self._pool.append(thread)
        # Start the thread, executing the given task in the background.
        thread.start()

# Create an instance of the `ThreadPool` class (singleton, so always returns the same instance).
thread_pool = ThreadPool()

# Add a new task to the thread pool, using a lambda function that prints "Task executed".
thread_pool.add_task(lambda: print("Task executed"))
