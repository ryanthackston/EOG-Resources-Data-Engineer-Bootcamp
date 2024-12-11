# Centralized Service Access: Provides centralized access to various services within an application.
# Service locator to find logger, ... goes through a dictionary and returns a reference to that resource

class ServiceLocator:
    # A class variable `_instance` is defined to store a single instance of the class (used for implementing the singleton pattern).
    _instance = None

    # The __new__ method is a special method in Python that creates a new instance of the class.
    # This is where we control the singleton behavior, ensuring only one instance of ServiceLocator exists.
    def __new__(cls):
        # If no instance exists, create a new instance.
        if cls._instance is None:
            # Use super() to call the parent class's __new__ method to actually create the instance.
            cls._instance = super(ServiceLocator, cls).__new__(cls)
            # Initialize an empty dictionary to store registered services.
            cls._instance.services = {}
        # Return the single instance of the class.
        return cls._instance
    
    # Method to register a service with a name. It takes `name` as the key and `service` as the service instance.
    def register_service(self, name, service):
        # Store the service in the dictionary with `name` as the key.
        self.services[name] = service

    # Method to retrieve a registered service by its name.
    def get_service(self, name):
        # Retrieve the service from the dictionary if it exists; otherwise, return None.
        return self.services.get(name)

# Create an instance of the ServiceLocator (will always return the same instance due to singleton pattern).
service_locator = ServiceLocator()

# Register a service with the name 'database' and a placeholder 'DatabaseService' string as the service.
service_locator.register_service('database', 'DatabaseService')

# Retrieve the 'database' service and print it. This should output 'DatabaseService' if registered successfully.
print(service_locator.get_service('database'))
