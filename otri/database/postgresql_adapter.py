from .database_adapter import DatabaseAdapter, DatabaseData, DatabaseQuery
from .database_stream import PostgreSQLStream
from ..utils import logger as log

import json
import psycopg2
from psycopg2.extras import execute_values
from typing import Union

class PostgreSQLAdapter(DatabaseAdapter):
    '''
    Database adapter for postgreSQL
    '''

    def __init__(self, username: str, password: str, host: str, port: Union[str,int] = "5432"):
        '''
        Initialises postgreSQL connection.

        Parameters:
            username : str
            password : str
            host : str
                IP or URL of the DB
            port : str
                Port where the DB is hosted
        '''
        try:
            log.i("Trying to connect to PGSQL Database")
            self.connection = psycopg2.connect(
                user=username, password=password, host=host, port=port)
            self.cursor = self.connection.cursor()
            log.i("Connected to PGSQL")

        except (Exception, psycopg2.Error) as error:
            log.e("Error while connecting to PostgreSQL: {}".format(error))

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
        if(type(data.values) == list):
            data_json_list = [(json.dumps(element),)
                              for element in data.values]
            execute_values(self.cursor, "INSERT INTO {} (data_json) VALUES %s".format(
                data.category), data_json_list)
            self.connection.commit()
            log.v("Upload completed")
        elif(type(data.values) == dict):
            self.cursor.execute("INSERT INTO {} (data_json) VALUES %s".format(
                data.category), (data.values,))
            self.connection.commit()
            log.v("Upload completed")
        else:
            raise ValueError("Data value not a list or a dict")

    def read(self, query: DatabaseQuery):
        '''
        Queries the database for the requested values.
        TODO: Define how a postgre JSON query should be formatted (https://devhints.io/postgresql-json)

        Parameters:
            query : DatabaseQuery
                Executes the query on the given query.category and the given query.filters
        Returns:
            list containing json dicts
        '''
        self.cursor.execute("SELECT data_json as json FROM {} WHERE {};".format(
            query.category, query.filters))
        return [json.dumps(element[0]) for element in self.cursor.fetchall()]

    def stream(self, query: DatabaseQuery, batch_size: int = 1000) -> PostgreSQLStream:
        '''
        Returns a database stream.

        Parameters:
            query : DatabaseQuery
                Executes the query on the given query.category and the given query.filters
            batch_size : int
                The number of rows the database should load before making them available.
                The iterable still always yields a single item.
        Returns:
            An Iterable stream of database rows that match the query.
        '''
        return PostgreSQLStream(self.connection, query, batch_size)

    def __create_table(self, table_name: str):
        '''
        Crates a new table, if it doesn't already exist.

        Parameters:
            table_name : str
                Name of the table to create.
        '''
        log.i("creating new pgSQL DB table {}".format(table_name))
        self.cursor.execute(
            "CREATE TABLE {} (id BIGSERIAL PRIMARY KEY, data_json JSON NOT NULL);".format(table_name))
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
        self.cursor.execute(
            "SELECT tablename FROM pg_catalog.pg_tables WHERE tablename = %s;", (table_name,))
        return len(self.cursor.fetchall()) > 0

    def close(self):
        '''
        Closes database connection.
        '''
        if(self.connection):
            log.i("closing pgSQL DB connection")
            self.cursor.close()
            self.connection.close()


class PostgreSQLSSH(PostgreSQLAdapter):
    '''
    Database adapter for postgreSQL through an SSH tunnel.
    '''

    def __init__(self, username: str, password: str, host: str,
                 ssh_user: str, ssh_password: str, ssh_host: str,
                 port: Union[str, int] = 5432, ssh_port: Union[str, int] = 22):
        '''
        Initialises postgreSQL connection.

        Parameters:
            username : str\n
            password : str\n
            host : str
                IP or URL of the DB, relative to the ssh gateway.\n
            ssh_user : str\n
            ssh_password : str\n
            ssh_host : str\n
            port : str
                Port where the DB is hosted\n
        '''
        # To keep sshtunnel optional. Re-importing is a no-op anyway.
        from sshtunnel import SSHTunnelForwarder
        # ---
        try:
            log.i("Trying to open SSH tunnel.")
            self.tunnel = SSHTunnelForwarder(
                (ssh_host, int(ssh_port)),
                ssh_username=ssh_user,
                ssh_password=ssh_password,
                remote_bind_address=(host, int(port))
            )
            self.tunnel.start()
            log.i("Tunnel opened on local port {}.".format(self.tunnel.local_bind_port))
            log.i("Trying to connect to PGSQL Database")
            self.connection = psycopg2.connect(
                user=username,
                password=password,
                host="localhost",
                port=self.tunnel.local_bind_port
            )
            self.cursor = self.connection.cursor()
            log.i("Connected to PGSQL")

        except (Exception, psycopg2.Error) as error:
            log.e("Error while connecting to PostgreSQL: {}".format(error))

    def close(self):
        '''
        Closes database connection and the associated tunnel.
        '''
        super().close()
        if (self.tunnel):
            self.tunnel.close()
            log.i("Closed SSH tunnel on local port {}.".format(self.tunnel.local_bind_port))