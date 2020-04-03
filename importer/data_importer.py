
from database.database_adapter import DatabaseAdapter

'''
Classes:
DataImporter -- Abstract class to import data from other sources
'''
class DataImporter:
    '''
        Abstract class, used to import data from an outside stream (REST API or WS) or from a file (JSON, XML) to an inside
        database of any kind (MongoDB, DynamoDB, Postrgres JSON, etc)

        Parameters:: 
            database : DatabaseAdapter

    '''
    def __init__(self, database : DatabaseAdapter):
        self.database = database