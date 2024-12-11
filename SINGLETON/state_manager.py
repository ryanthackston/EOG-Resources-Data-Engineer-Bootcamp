# USE CASE: STATE MANAGER
# Global State: Manages application-wide state to ensure consistency across the application.




# Define a class named `AppState` that serves as a global state manager for the application, using the singleton pattern.
class AppState:
    # A class variable `_instance` is defined to hold the single instance of the `AppState` class.
    _instance = None

    # The __new__ method is a special method that creates a new instance of the class.
    # It is overridden here to implement the singleton pattern, ensuring only one instance of `AppState` exists.
    def __new__(cls):
        # Check if `_instance` is None, meaning no instance of `AppState` has been created yet.
        if cls._instance is None:
            # If no instance exists, create one by calling the parent class's __new__ method.
            cls._instance = super(AppState, cls).__new__(cls)
            # Initialize an empty dictionary `state` to store global state variables.
            cls._instance.state = {}
        # Return the single instance of the class.
        return cls._instance

    # Method to set a state variable, given a `key` and a `value`.
    def set_state(self, key, value):
        # Store the `value` in `state` with the specified `key`.
        self.state[key] = value

    # Method to retrieve the value of a state variable by `key`.
    def get_state(self, key):
        # Return the value associated with the `key` from `state`, or None if the key does not exist.
        return self.state.get(key)

# Create an instance of the `AppState` class (will always return the same instance due to singleton pattern).
app_state = AppState()

# Set a global state variable `user_logged_in` with a value of `True`.
app_state.set_state('user_logged_in', True)

# Retrieve and print the value associated with `user_logged_in` in the global state, which should output `True`.
print(app_state.get_state('user_logged_in'))
