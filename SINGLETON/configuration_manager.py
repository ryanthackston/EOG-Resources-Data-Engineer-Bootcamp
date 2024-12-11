# __new__ when you define a new instance of that class



class ConfigurationManager:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(ConfigurationManager, cls).__new__(cls)
            cls._instance.config = cls.load_config()
        return cls._instance
    
    @staticmethod
    def load_config():
        return {
            'setting1': 'value1',
            'setting2':'value2'
        }

# Creating an instance right here. Invokes __new__(cls) routine... 
config = ConfigurationManager()
print(config.config)