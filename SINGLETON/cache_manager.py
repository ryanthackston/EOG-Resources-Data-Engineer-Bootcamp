# Define a class named `Cache` that implements a singleton pattern for storing key-value pairs.
class Cache:
    # A class variable `_instance` is defined to hold the single instance of the `Cache` class.
    _instance = None

    # The __new__ method is a special method that creates a new instance of the class.
    # It is overridden here to implement the singleton pattern.
    def __new__(cls):
        # Check if `_instance` is None, meaning no instance of `Cache` exists yet.
        if cls._instance is None:
            # If no instance exists, create one by calling the parent class's __new__ method.
            cls._instance = super(Cache, cls).__new__(cls)
            # Initialize an empty dictionary `storage` to hold key-value pairs in the singleton instance.
            cls._instance.storage = {}
        # Return the single instance of the class.
        return cls._instance

    # Method to add a key-value pair to the cache.
    def put(self, key, value):
        # Store the value in `storage` with the specified `key`.
        self.storage[key] = value

    # Method to retrieve a value by key from the cache.
    def get(self, key):
        # Return the value associated with the `key` from `storage`, or None if the key does not exist.
        return self.storage.get(key)

# Create an instance of the `Cache` class (will always return the same instance due to singleton pattern).
cache = Cache()

# Add a key-value pair ('key1', 'value1') to the cache.
cache.put('key1', 'value1')

# Retrieve and print the value associated with 'key1' in the cache, which should output 'value1'.
print(cache.get('key1'))