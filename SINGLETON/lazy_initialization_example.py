import threading

class Singleton:
    _instance = None
    _lock = threading.Lock()

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:  # Double-checked locking
                    cls._instance = super(Singleton, cls).__new__(cls)  # Only pass cls here
                    cls._instance.initialize(*args, **kwargs)  # Initialize with any arguments
        return cls._instance

    def initialize(self, *args, **kwargs):
        # Your initialization logic here, using args or kwargs if necessary
        self.value = kwargs.get('value', None)

# Usage example
singleton1 = Singleton(value='First Instance')
print(singleton1.value)

singleton2 = Singleton(value='Second Instance')
print(singleton2.value)

print(singleton1 is singleton2) # Output: True, confirming that both are the same instance