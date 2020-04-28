
from database.database_adapter import DatabaseAdapter
from downloader.timeseries_downloader import META_INTERVAL_KEY, META_PROVIDER_KEY, META_TICKER_KEY

'''
Keys to grab from metadata and append to every atom
'''
METADATA_ATOM_KEYS = [META_INTERVAL_KEY, META_PROVIDER_KEY, META_TICKER_KEY]

class DataImporter:
    '''
    Abstract class, used to import data from an outside stream (REST API or WS) or from a file (JSON, XML) to a
    database of any kind (MongoDB, DynamoDB, Postrgres JSON, etc)

    Attributes:
        database : DatabaseAdapter
            Adapter for whatever database it'll be using to store imported data
    '''
    def __init__(self, database : DatabaseAdapter):
        '''
        Constructor method, requires database connection.

        Parameters:
            database : DatabaseAdapter
                Adapter for the database where to store the imported data
        '''
        self.database = database