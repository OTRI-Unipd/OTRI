from typing import Any, Tuple, Mapping
from ..filtering.stream import ReadableStream

__version__ = "2.0"
__author__ = "Riccardo De Zen <riccardodezen98@gmail.com>"


class DatabaseStream(ReadableStream):
    '''
    A stream that contains data from the database.

    TODO: find a meaning for this class.
    '''
    pass


class PostgreSQLStream(DatabaseStream):

    __CURSOR_NAME = "otri_cursor_{}"
    __CURSOR_ID = 0  # Static cursor ID variable

    def __init__(self, connection, query: str, batch_size: int = 1000, extract_atom: bool = False):
        '''
        Parameters:\n
            connection : psycopg2.connection
                A connection to the desired database.\n
            query : str
                The query to stream.\n
            batch_size : int = 1000
                The amount of rows to fetch each time the cached rows are read.\n
            extract_atom : bool
                Whether the popped elements are only atoms or the whole database tuple.\n
        Raises:\n
            psycopg2.errors.* - if the query is not correct due to syntax or wrong names.
        '''
        super().__init__()
        self.__connection = connection
        self.__cursor = self.__new_cursor(connection, batch_size)
        self.__cursor.execute(query)
        self.__buffer = None
        self.__extract = extract_atom

    def _pop(self) -> Union[Tuple, Mapping]:
        '''
        Reads one element from the cursor or from the local buffer.

        Returns:
            An atom if extract_atom is set to True, a row (tuple) otherwise.
        '''
        if self.__cursor.closed:
            raise IndexError("PostgreSQLStream is empty")
        if self.__buffer is not None:
            item = self.__buffer
            self.__buffer = None  # Empties the buffer
            if self.__extract:
                return item[1] # [0] is ID, [1] is atom
            return item
        else:
            try:
                return next(self.__cursor)
            except StopIteration:
                # No more data and has_next() wasn't checked, raise IndexError
                self.close()
                raise IndexError("PostgreSQLStream is empty")

    def has_next(self) -> bool:
        if self.__buffer is not None: # Buffer is not empty there sure is a next
            return True
        try:
            self.__buffer = next(self.__cursor) # Cursor gave something, there is next
            return True
        except StopIteration: # Cursor raised exception, close it, no next.
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
            A new cursor with a guaranteed unique name for this stream.
        '''
        name = PostgreSQLStream.__CURSOR_NAME.format(
            PostgreSQLStream.__CURSOR_ID)
        PostgreSQLStream.__CURSOR_ID += 1
        cursor = connection.cursor(name)
        cursor.itersize = batch_size
        return cursor
