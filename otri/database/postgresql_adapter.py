"""
This module contains two `DatabaseAdapter` subclasses:

- `PostgreSQLAdapter`: An adapter specifically made for PostgreSQL, allows conversion of a query to
a queue.
- `PostgreSQLSSH`: Inherits from the first one, but uses ssh tunneling to access its target.
"""

__author__ = "Riccardo De Zen <riccardodezen98@gmail.com>"
__version__ = "2.0"

from .database_adapter import DatabaseAdapter
from .database_queue import PostgreSQLQueue
from ..utils import logger as log

from typing import Union

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm.query import Query


class PostgreSQLAdapter(DatabaseAdapter):
    '''
    Database adapter for postgreSQL
    '''

    def __init__(self, password: str, host: str = "localhost", port: Union[str, int] = 5432,
                 user: str = "postgres", database: str = "postgres"):
        '''
        Parameters:
            host : str
                The database host address. Defaults to "localhost".\n
            port : Union[str, int]
                The port on which the database is listening. Defaults to 5432.\n
            user : str
                The username. Defaults to "postgres".\n
            password : str
                The password for the given user. Has no default value.\n
            database : str
                The database on which to connect. Defaults to "postgres".
        '''
        super().__init__(password, host, port, user, database)

    def queue(self, query: Query, batch_size: int = 1000) -> PostgreSQLQueue:
        '''
        Returns a database queue that performs the given query fetching `batch_size` rows at a
        time. If you need to fetch few rows at a time but do not need a `Queue` object use
        sqlalchemy's `yield_per(amount)` in a session.
        A new connection is opened for each queue.

        Parameters:
            query : Query
                Query to run, must be an sqlalchemy object, must be a read only query. A new
                connection is opened in order to run it, so if you got the query from a preexisting
                session, you don't need to keep it open.\n
            batch_size : int
                The number of rows the database should load before making them available.
                The iterable still always yields a single item.\n
        Returns:
            An Iterable queue of database rows that match the query.
        '''
        if not isinstance(query, Query):
            raise ValueError("Not an SQLAlchemy query.")

        query = query.statement.compile(self._engine, compile_kwargs={"literal_binds": True}).string
        return PostgreSQLQueue(self._engine.raw_connection(), query, batch_size)

    def _connection_string(self) -> str:
        '''
        Returns:
            Connection string for a PostgreSQL database using psycopg2 driver.
            Format fields are, in order: user, password, host, port, database.
        '''
        return "postgresql+psycopg2://{}:{}@{}:{}/{}"

    def _make_engine(self, conn_str: str) -> Engine:
        '''
        Retrieve an engine for the given PostgreSQL connection string.

        Parameters:
            conn_str : str
                The connection string for the desired database.
        Returns:
            An engine for the given connection string. This engine will have psycopg2's bulk
            insertion helpers enabled. Default executemany mode is 'values', with a values page size
            of 10000 and a batch page size of 500.
        '''
        # executemany_mode = 'values' -> Use `execute_values` if possible, else `execute_batch`.
        return create_engine(
            conn_str,
            executemany_mode="values",
            executemany_values_page_size=10000,
            executemany_batch_page_size=500
        )


class PostgreSQLSSH(PostgreSQLAdapter):
    '''
    Database adapter for postgreSQL through an SSH tunnel.
    '''

    def __init__(self, ssh_user: str, ssh_password: str, ssh_host: str, password: str,
                 host: str = "localhost", port: Union[str, int] = 5432, user: str = "postgres",
                 database: str = "postgres", ssh_port: Union[str, int] = 22):
        '''
        Initialises postgreSQL connection.

        Parameters:
            Parameters:
            host : str
                The database host address. Defaults to "localhost".\n
            port : Union[str, int]
                The port on which the database is listening. Defaults to 5432.\n
            user : str
                The username. Defaults to "postgres".\n
            password : str
                The password for the given user. Has no default value.\n
            database : str
                The database on which to connect. Defaults to "postgres".\n
            ssh_user : str
                The ssh user.\n
            ssh_host : str
                The ssh host to use when performing the tunnel.\n
            ssh_password : str
                The ssh password for the given user.\n
            ssh_port : Union[str, int]
                The port on which to connect via ssh, defaults to 22.
        '''
        # To keep sshtunnel optional. Re-importing is a no-op anyway.
        from sshtunnel import SSHTunnelForwarder
        # ---
        try:
            log.i("trying to open SSH tunnel.")
            self.tunnel = SSHTunnelForwarder(
                (ssh_host, int(ssh_port)),
                ssh_username=ssh_user,
                ssh_password=ssh_password,
                remote_bind_address=(host, int(port))
            )
            self.tunnel.daemon_forward_servers = True
            self.tunnel.start()
            log.i("tunnel opened on local port {}.".format(self.tunnel.local_bind_port))
            super().__init__(password, "localhost", self.tunnel.local_bind_port, user, database)

        except Exception as error:
            log.e("error while connecting to PostgreSQL: {}".format(error))
            raise

    def close(self):
        '''
        Closes the tunnel, any further operation will fail.
        '''
        log.i("attempting to close SSH tunnel on local port {}.".format(self.tunnel.local_bind_port))
        self.tunnel.stop()
        log.i("closed SSH tunnel.")
