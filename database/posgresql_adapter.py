from database.database_adapter import DatabaseAdapter, DatabaseData, DatabaseQuery
from config import Config
import json
import psycopg2
from psycopg2.extras import execute_values


class PosgreSQLAdapter(DatabaseAdapter):
    '''
    Database adapter for postgreSQL
    '''

    def __init__(self):
        try:
            print("Trying to connect to PGSQL Database")
            config = Config()
            self.connection = psycopg2.connect(user=config.get_config("postgre_username"),
                                               password=config.get_config(
                                                   "postgre_password"),
                                               host=config.get_config(
                                                   "postgre_host"),
                                               port="5432")
            self.cursor = self.connection.cursor()
            #self.cursor.execute("SELECT tablename FROM pg_catalog.pg_tables;")
            #print(self.cursor.fetchall())
            print("Connected to PGSQL")

        except (Exception, psycopg2.Error) as error:
            print("Error while connecting to PostgreSQL", error)

    def write(self, data: DatabaseData):
        '''
        Writes data.values inside the database in the data.category (could be a table ? or a row ?)

        Parameters:
            data : DatabaseData
                Data to write in DB, could be a list or a dict
        Raises:
            ValueError
                If data.values is not a list or a dict
        '''
        if(not self.__table_exists(data.category)):
            self.__create_table(data.category)

        if(type(data.values) == list):
            data_json_list = [(json.dumps(element),) for element in data.values]
            execute_values(self.cursor, "INSERT INTO {} (data_json) VALUES %s".format(data.category), data_json_list)
            self.connection.commit()
            print("Upload completed")
        elif(type(data.values) == dict):
            self.cursor.execute("INSERT INTO {} (data_json) VALUES %s".format(data.category), (data.values,))
            self.connection.commit()
        else:
            raise ValueError("Data value not a list or a dict")

    def read(self, query: DatabaseQuery):
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

    def __create_table(self, table_name: str):
        '''
        Crates a new table, if it doesn't already exist.

        Parameters:
            table_name : str
                Name of the table to create.
        '''
        print("Creating pgSQL DB {}".format(table_name))
        self.cursor.execute("CREATE TABLE {} (id BIGSERIAL PRIMARY KEY, data_json JSON NOT NULL);".format(table_name))
        self.connection.commit()

    def __table_exists(self, table_name: str):
        '''
        Determines if the given table exists.

        Parameters:
            table_name : str
                Table name to check the existance of.
        Returns:
            True if table exists, False otherwise.
        '''
        self.cursor.execute("SELECT tablename FROM pg_catalog.pg_tables WHERE tablename = %s;", (table_name,))
        return len(self.cursor.fetchall()) > 0

    def close(self):
        '''
        Closes database connection.
        '''
        if(self.connection):
            self.cursor.close()
            self.connection.close()
