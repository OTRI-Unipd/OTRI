"""
This module contains two `DatabaseAdapter` subclasses:
- `PostgreSQLAdapter`: An adapter specifically made for PostgreSQL, allows conversion of a query to
a stream.
- `PostgreSQLSSH`: Inherits from the first one, but uses ssh tunneling to access its target.
"""
from .database_adapter import DatabaseAdapter
from .database_stream import PostgreSQLStream
from ..utils import logger as log

from typing import Union

from sqlalchemy.orm.query import Query


class PostgreSQLAdapter(DatabaseAdapter):
    '''
    Database adapter for postgreSQL
    '''

    def __init__(self, password: str, host: str = "localhost", port: Union[str, int] = 5432,
                 user: str = "postgres", database: str = "postgres"):
        '''
        Parameters:
            host : str\n
                The database host address. Defaults to "localhost".\n
            port : Union[str, int]\n
                The port on which the database is listening. Defaults to 5432.\n
            user : str\n
                The username. Defaults to "postgres".\n
            password : str\n
                The password for the given user. Has no default value.\n
            database : str\n
                The database on which to connect. Defaults to "postgres".
        '''
        super().__init__(password, host, port, user, database)

    def stream(self, query, batch_size: int = 1000) -> PostgreSQLStream:
        '''
        Returns a database stream that performs the given query fetching `batch_size` rows at a
        time. If you need to fetch few rows at a time but do not need a `Stream` object use
        sqlalchemy's `yield_per(amount)` in a session.
        A new connection is opened for each stream.

        Parameters:
            query\n
                Query to run, must be an sqlalchemy object, must be a read only query.\n A new
                connection is opened in order to run it, so if you got the query from a preexisting
                session, you don't need to keep it open.\n
            batch_size : int\n
                The number of rows the database should load before making them available.\n
                The iterable still always yields a single item.\n
        Returns:
            An Iterable stream of database rows that match the query.
        '''
        if not isinstance(query, Query):
            raise ValueError("Not an SQLAlchemy query.")

        query = query.statement.compile(self._engine, compile_kwargs={"literal_binds": True}).string
        return PostgreSQLStream(self._engine.raw_connection(), query, batch_size)

    def _connection_string(self):
        '''
        Returns:\n
            Connection string for a PostgreSQL database using psycopg2 driver.
            Format fields are, in order: user, password, host, port, database.
        '''
        return "postgresql+psycopg2://{}:{}@{}:{}/{}"


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
            host : str\n
                The database host address. Defaults to "localhost".\n
            port : Union[str, int]\n
                The port on which the database is listening. Defaults to 5432.\n
            user : str\n
                The username. Defaults to "postgres".\n
            password : str\n
                The password for the given user. Has no default value.\n
            database : str\n
                The database on which to connect. Defaults to "postgres".\n
            ssh_user : str\n
                The ssh user.\n
            ssh_host : str\n
                The ssh host to use when performing the tunnel.\n
            ssh_password : str\n
                The ssh password for the given user.\n
            ssh_port : Union[str, int]\n
                The port on which to connect via ssh, defaults to 22.
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
            self.tunnel.daemon_forward_servers = True
            self.tunnel.start()
            log.i("Tunnel opened on local port {}.".format(self.tunnel.local_bind_port))
            super().__init__(password, "localhost", self.tunnel.local_bind_port, user, database)

        except Exception as error:
            log.e("Error while connecting to PostgreSQL: {}".format(error))
            raise

    def close(self):
        '''
        Closes the tunnel, any further operation will fail.
        '''
        log.i("Attempting to close SSH tunnel on local port {}.".format(self.tunnel.local_bind_port))
        self.tunnel.stop()
        log.i("Closed SSH tunnel.")
