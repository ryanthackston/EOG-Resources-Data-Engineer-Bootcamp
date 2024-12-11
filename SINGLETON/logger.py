class Logger:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(Logger, cls).__new__(cls)
            cls._instance.log_file = open('app.log', 'a')
        return cls._instance
    
    def log(self, message): 
        self._instance.log_file.write(message + '\n')

logger = Logger()
logger.log("This is a log message")