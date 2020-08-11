"""
Basic SQLAlchemy wrapper providing some common functionalities for interfacing with a database.
"""

__author__ = "Riccardo De Zen <riccardodezen98@gmail.com>"
__version__ = "2.1"

from typing import List, Mapping, Union
from contextlib import contextmanager
from sqlalchemy import *
from sqlalchemy.engine import Engine
from sqlalchemy.schema import MetaData, Table
from sqlalchemy.orm import sessionmaker
from sqlalchemy.engine import Transaction
from sqlalchemy.orm.session import Session
from sqlalchemy.ext.automap import automap_base

from ..utils import logger as log


class DatabaseAdapter:
    '''
    Abstract class used to access with the same methods independently from the kind of database used.
    Uses SQLAlchemy to interface with the database.
    '''

    def __init__(self, password: str, host: str, port: Union[str, int], user: str, database: str):
        '''
        Parameters:
            host : str
                The database host address.\n
            port : Union[str, int]
                The port on which the database is listening.\n
            user : str
                The username.\n
            password : str
                The password for the given user.\n
            database : str
                The database on which to connect.
        '''
        try:
            # Store for string representation.
            self._host = host
            self._database = database

            # Connect
            log.i("Trying to connect to database.")
            conn_str = self._connection_string().format(
                user,
                password,
                host,
                port,
                database
            )
            self._engine = self._make_engine(conn_str)

            # Sqlalchemy Core Metadata.
            self._meta = MetaData()
            self._meta.reflect(bind=self._engine)

            # ORM objects.
            self._Base = automap_base()
            self._Base.prepare(self._engine, reflect=True)
            self._Session = sessionmaker(bind=self._engine)

            log.i("connected to the database.")

        except:
            log.e("error while connecting to the database")
            raise

    def get_tables(self) -> Mapping[str, Table]:
        '''
        Retrieve the SQLAlchemy reflected tables for the db.

        Returns:
            A dictionary containing the tables for the database. The keys are the tables' names.
        '''
        return self._meta.tables

    def get_classes(self) -> Mapping:
        '''
        Retrieve ORM classes for the reflected tables in the db.

        Returns:
            A dictionary containing the classes for the tables. The keys are the tables' names.
            The classes can be found also as attributes, so both the following are acceptable:
            ```python
            classes.table
            classes["table"]
            ```
        '''
        return self._Base.classes

    def insert(self, table: Union[str, Table], values: List[Mapping]):
        '''
        Add an arbitrary number of elements (`values`) to `table` in a single transaction and commit.

        Parameters:
            table : Union[str, Table]
                The database table on which to perform the inserts.\n
            values : List[Mapping]
                The values to insert in the table. They need to be dictionaries or similar mappings
                representing a db row: `row['column'] = value`.
        '''
        log.d("Attempting to insert {} values in {}".format(len(values), table))
        if isinstance(table, str):
            table = self.get_tables()[table]
        elif table not in self.get_tables():
            log.e("INSERT failed: Table not in database.")
        try:
            with self._engine.begin() as conn:
                conn.execute(table.insert(), values)
        except:
            log.e("Error during upload to {}. Rolling back...".format(self))
            raise
        log.d("Upload to {} successful.".format(self))

    @contextmanager
    def begin(self) -> Transaction:
        '''
        Context Manager (can use in `with` blocks), opens a connection to operate on the database.
        The connection also opens a transaction, which is commited if the `with` block exits
        successfully and rolled back if an exception occurs.

        Use this if you need to query or delete.
        '''
        connection = self._engine.connect()
        transaction = connection.begin()
        try:
            yield transaction
            # Commit when with block exits.
            transaction.commit()
        except:
            # Rollback if exception.
            transaction.rollback()
            log.e("error during transaction on {}. Rolling back...".format(self))
            raise
        finally:
            # Close either way.
            transaction.close()
            connection.close()

    @contextmanager
    def session(self) -> Session:
        '''
        Similar to `begin()`, opens a Session. Commits if the `with` block exits successfully, rolls
        back otherwise.

        Use this if you prefer the ORM approach.

        Returns:
            A `Session` object for this db.
        '''
        session = self._Session()
        try:
            yield session
            # Commit when with block exits.
            session.commit()
        except:
            # Rollback if exception.
            session.rollback()
            log.e("error during session on {}. Rolling back...".format(self))
            raise
        finally:
            # Close either way.
            session.close()

    def _connection_string(self) -> str:
        '''
        Retrieve an appropriate connection String. Implement this method in subclasses.
        See: ![SQLAlchemy's docs](https://docs.sqlalchemy.org/en/13/core/engines.html#database-urls)
        for details.

        Returns:
            A connection string, based on the database type and engine to use, ready to be formatted
            with the user info.
        '''
        raise NotImplementedError("This is an abstract method, please implement it in a sub-class")

    def _make_engine(self, conn_str: str) -> Engine:
        '''
        Retrieve an engine for the given connection string. Override this method if you need more
        options.

        Parameters:
            conn_str : str
                The connection string for the desired database.
        Returns:
            An engine for the given connection string.
        '''
        return create_engine(conn_str)

    def __repr__(self):
        '''
        Returns:
            str representation of the object. The database host and db name.
        '''
        return str("{{Host: {}, Database: {}}}".format(self._host, self._database))
