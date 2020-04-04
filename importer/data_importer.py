
from database.database_adapter import DatabaseAdapter

class DataImporter:
    '''
        Abstract class, used to import data from an outside stream (REST API or WS) or from a file (JSON, XML) to an inside
        database of any kind (MongoDB, DynamoDB, Postrgres JSON, etc)

        Parameters:
            database : DatabaseAdapter
                Adapter for whatever database we'll be using
    '''
    def __init__(self, database : DatabaseAdapter):
        self.database = database