"""
Basic SQLAlchemy wrapper providing some common functionalities for interfacing with a database.
"""

from typing import List, Mapping, Union
from contextlib import contextmanager
from sqlalchemy import *
from sqlalchemy.orm import sessionmaker
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
            host : str\n
                The database host address.\n
            port : Union[str, int]\n
                The port on which the database is listening.\n
            user : str\n
                The username.\n
            password : str\n
                The password for the given user.\n
            database : str\n
                The database on which to connect.
        '''
        try:
            log.i("Trying to connect to database.")
            conn_str = self._connection_string().format(
                user,
                password,
                host,
                port,
                database
            )
            self._engine = create_engine(conn_str)
            self._Base = automap_base()
            self._Base.prepare(self._engine, reflect=True)
            self._Session = sessionmaker(bind=self._engine)
            log.i("Connected to the database.")

        except:
            log.e("Error while connecting to the database")
            raise

    def get_tables(self) -> Mapping:
        '''
        Retrieve the SQLAlchemy reflected tables for the db.

        Returns:
            A dictionary containing the tables for the database. The tables can be retrieved both as
            string keys and attributes. So, for a table named "users":
            ```python
            adapter.get_tables()["users"]
            adapter.get_tables().users
            ```
            Are both valid.
        '''
        return self._Base.classes

    def add(self, value):
        '''
        Add a single value to the database and commit the change.

        Parameters:
            value\n
                Must be an object from one of the already existing tables retrieved via
                `DatabaseAdapter.get_tables()`.
        '''
        session = self._Session()
        try:
            session.add(value)
            session.commit()
            log.v("Uploaded an item to {} database.".format(self))
        except:
            session.rollback()
            log.e("Error during upload to {}. Rolling back...".format(self))
            raise
        finally:
            session.close()

    def add_all(self, values: List):
        '''
        Same as `DatabaseAdapter.add(value)` for multiple values. The change is immediately
        committed.

        Parameters:
            values : List\n
                List of values to add to the db, they must be objects from existing tables.
        '''
        session = self._Session()
        log.i("Opened session on {}".format(self))
        try:
            session.add_all(values)
            log.i("Added items to {} session.".format(self))
            session.commit()
            log.i("Uploaded items to {} database.".format(self))
        except:
            session.rollback()
            log.e("Error during upload to {}. Rolling back...".format(self))
            raise
        finally:
            session.close()

    def delete(self, value):
        '''
        Delete a value from the database and immediately commit.

        Parameters:
            value\n
                Must be an object from one of the already existing tables retrieved via
                `DatabaseAdapter.get_tables()`.
        '''
        session = self._Session()
        try:
            session.delete(value)
            session.commit()
            log.v("Removed items from {} database.".format(self))
        except:
            session.rollback()
            log.e("Error during removal from {}. Rolling back...".format(self))
            raise
        finally:
            session.close()

    def delete_all(self, values: List):
        '''
        Delete some values from the database and commit if all got deleted.

        Parameters:
            values : List\n
                List of values to remove from the db, they must be objects from existing tables.
        '''
        session = self._Session()
        try:
            for v in values:
                session.delete(v)
            session.commit()
            log.v("Removed items from {} database.".format(self))
        except:
            session.rollback()
            log.e("Error during removal from {}. Rolling back...".format(self))
            raise
        finally:
            session.close()

    def query(self, *items):
        '''
        Return a read-only query.
        '''

    @contextmanager
    def session(self):
        """
        Context Manager (can use in `with` blocks), opens a session to operate on the database.
        Once the session is open, you can still perform operations on the database directly, but
        they will not be included in the transaction of this session.

        If using this in a `with` block, if no error arises the session commits when exiting the
        block, otherwise it calls `rollback()`.

        Use this if you need any querying or advanced operations.
        """
        session = self._Session()
        try:
            yield session
            # Commit when with block exits.
            session.commit()
        except:
            # Rollback if exception.
            session.rollback()
            log.e("Error during session on {}. Rolling back...".format(self))
            raise
        finally:
            # Close either way.
            session.close()

    def _connection_string(self) -> str:
        '''
        Retrieve an appropriate connection String. Implement this method in subclasses.

        Returns:
            A connection string, based on the database type and engine to use, ready to be formatted
            with the user info.
        '''
        raise NotImplementedError("This is an abstract method, please implement it in a sub-class")
