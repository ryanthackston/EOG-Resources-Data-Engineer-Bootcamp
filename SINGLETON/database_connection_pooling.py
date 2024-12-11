class DatabaseConnectionPool:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(DatabaseConnectionPool, cls).__new__(cls)
            cls._instance.connections = cls.create_pool()
        return cls._instance
    
    @staticmethod
    def create_pool():
        return ['conn1', 'conn2', 'conn3']
    
    def get_connection(self):
        return self.connections.pop()
    
db_pool = DatabaseConnectionPool()
print(db_pool.get_connection())