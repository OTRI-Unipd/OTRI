from typing import Any
from ..filtering.queue import ReadableQueue

__version__ = "2.0"
__author__ = "Riccardo De Zen <riccardodezen98@gmail.com>"


class DatabaseQueue(ReadableQueue):
    '''
    A queue that contains data from the database.

    TODO: find a meaning for this class.
    '''
    pass


class PostgreSQLQueue(DatabaseQueue):

    __CURSOR_NAME = "otri_cursor_{}"
    __CURSOR_ID = 0  # Static cursor ID variable

    def __init__(self, connection, query: str, batch_size: int = 1000):
        '''
        Parameters:\n
            connection : psycopg2.connection
                A connection to the desired database.\n
            query : str
                The query to queue.\n
            batch_size : int = 1000
                The amount of rows to fetch each time the cached rows are read.\n
        Raises:\n
            psycopg2.errors.* - if the query is not correct due to syntax or wrong names.
        '''
        super().__init__()
        self.__connection = connection
        self.__cursor = self.__new_cursor(connection, batch_size)
        self.__cursor.execute(query)
        self.__buffer = None

    def _pop(self) -> Any:
        '''
        Reads one element from the cursor or from the local buffer.
        '''
        if self.__cursor.closed:
            raise IndexError("PostgreSQLQueue is empty")
        if self.__buffer is not None:
            item = self.__buffer
            self.__buffer = None  # Empty the buffer
            return item
        else:
            try:
                return next(self.__cursor)
            except StopIteration:
                # No more data and has_next() wasn't checked, raise IndexError144
                self.close()
                raise IndexError("PostgreSQLQueue is empty")

    def has_next(self) -> bool:
        if self.__buffer is not None:
            return True
        try:
            self.__buffer = next(self.__cursor)
            return True
        except StopIteration:
            self.close()
            return False

    def close(self):
        super().close()
        self.__connection.close()

    def __new_cursor(self, connection, batch_size: int):
        '''
        Parameters:\n
            connection
                The connection from which to create the cursor.\n
            batch_size : int
                Size of the cursor fetched rows per step.\n
        Returns:\n
            A new cursor with a guaranteed unique name for this queue.
        '''
        name = PostgreSQLQueue.__CURSOR_NAME.format(
            PostgreSQLQueue.__CURSOR_ID)
        PostgreSQLQueue.__CURSOR_ID += 1
        cursor = connection.cursor(name)
        cursor.itersize = batch_size
        return cursor
