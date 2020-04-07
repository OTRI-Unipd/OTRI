from database.database_adapter import DatabaseAdapter, DatabaseData, DatabaseQuery
from config import Config
import psycopg2

class PosgreSQLAdapter(DatabaseAdapter):
    '''
    Database adapter for postgreSQL
    '''

    def __init__(self):
        try:
            print("Trying to connect to PostgreSQL Database")
            config = Config()
            self.connection = psycopg2.connect(user = config.get_config("postgre_username"),
                                  password = config.get_config("postgre_password"),
                                  host = config.get_config("postgre_host"),
                                  port = "5432",
                                  database = config.get_config("postgre_database"))
        except (Exception, psycopg2.Error) as error :
            print ("Error while connecting to PostgreSQL", error)

    def write(self, data : DatabaseData):
        '''
        Writes data.values inside the database in the data.category (could be a table ? or a row ?)

        Parameters:
            data : DatabaseData
                Data to write in DB
        '''
        print("Write")
    
    def read(self, query : DatabaseQuery):
        '''
        Queries the database for the requested values.
        TODO: Define how a postgre JSON query should be formatted

        Parameters:
            query : DatabaseQuery
                Executes the query on the given query.category and the given query.filters
        Returns:
            dict containing queried data
        '''
        print("read")
        return dict()