from typing import Tuple, Any, Iterable
from ..filtering.stream import Stream

class DatabaseStream(Stream):
    '''
    A stream that can't have data pushed in, only read from the database.
    '''
    def __init__(self):
        '''
        Avoids calling Stream class init if subclass doesn't override it.
        '''
        pass

    def __eq__(self, other):
        '''
        Avoids
        '''
        pass

    def push(self, element : Any):
        raise RuntimeError("cannot push data into a DatabaseStream")

    def push_all(self, elements : Iterable):
        raise RuntimeError("cannot push data into a DatabaseStream")

    def has_next(self):
        raise NotImplementedError("DatabaseStream is an abstract class, implement has_next in a sublcass")

    def clear(self):
        raise RuntimeError("cannot clear a DatabaseStream")

class PostgreSQLStream(DatabaseStream):

    __CURSOR_NAME = "otri_cursor_{}"
    __CURSOR_ID = 0

    def __init__(self, connection, query: str, batch_size: int = 1000):
        '''
        Parameters:\n
            connection : psycopg2.connection
                A connection to the desired database.\n
            query : str
                The query to stream.\n
            batch_size : int = 1000
                The amount of rows to fetch each time the cached rows are read.\n
        Raises:\n
            psycopg2.errors.* :
                if the query is not correct due to syntax or wrong names.
        '''
        super().__init__()
        self.__connection = connection
        self.__cursor = self.__new_cursor(connection, batch_size)
        self.__cursor.execute(query)
        self.__buffer = None

    def pop(self)-> Any:
        '''
        Returns:
            The first element of the given query result.\n
        Raises:
            IndexError - if there is no data available.
        '''
        if self.__cursor.closed:
            raise IndexError("DatabaseStream empty")
        if self.__buffer != None:
            item = self.__buffer
            self.__buffer = None
            return item
        else:
            try:
                return next(self.__cursor)
            except StopIteration:
                self.close()
                raise

    def has_next(self)->bool:
        '''
        Returns:
            True if the stream contains data, false otherwise.\n
        '''
        if self.__buffer != None:
            return True
        try:
            self.__buffer = next(self.__cursor)
            return True
        except StopIteration:
            self.close()
            return False

    def close(self):
        '''
        Prevents the stream from getting new data, data contained can still be iterated.
        '''
        super().close()
        self.__connection.close()

    def __new_cursor(self, connection, batch_size : int):
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
        cursor.itersize = self.__batch_size
        return cursor
