from .database_query import DatabaseQuery
from typing import Tuple


class DatabaseIterator():
    def __next__(self) -> Tuple:
        '''
        Returns:
            The next element in the sequence, fecthing it from the database.
        Except:
            StopIteration
                When no result is given by the database (the query is over).
        '''
        pass

    def has_next(self) -> bool:
        '''
        Returns:
            True - if the stream has a next item.
            False - if the stream has no other item.
        '''
        pass


class DatabaseStream():

    def __iter__(self) -> DatabaseIterator:
        '''
        Returns:
            The iterator for this object, returns a __DatabaseIterator object
        '''
        pass


class _PostgreSQLIterator(DatabaseIterator):

    def __init__(self, cursor):
        '''
        Parameters:
            cursor
                A cursor on which the query to iterate through has already been executed without fail.
        '''
        super().__init__()
        self.__cursor = cursor

    def __next__(self):
        '''
        Returns:
            The next element in the sequence, fecthing it from the database.
        Except:
            StopIteration
                When no result is given by the database (the query is over),
                closes the cursor and raises StopIteration again.
        '''
        try:
            return next(self.__cursor)
        except StopIteration:
            self.__cursor.close()
            raise StopIteration

    def has_next(self) -> bool:
        '''
        Returns:
            True if the cursor is still open (there are still rows to retrieve)
            False if the cursor is closed (the query is over)
        '''
        return not self.__cursor.closed

    def close(self):
        '''
        Closes the cursor.
        '''
        self.__cursor.close()


class PostgreSQLStream(DatabaseStream):

    __CURSOR_NAME = "otri_cursor_{}"
    __CURSOR_ID = 0

    def __init__(self, connection, query: DatabaseQuery, batch_size: int = 1000):
        '''
        Parameters:
            cursor : psycopg2.cursor
                A named cursor that can stream the given query.
            query : DatabaseQuery
                The query to stream.
        '''
        super().__init__()
        self.__conn = connection
        self.__query = query
        self.__batch_size = batch_size

    def __iter__(self) -> _PostgreSQLIterator:
        '''
        Returns:
            The iterator for this object.
            Can be its own iterator.
        Raises:
            psycopg2.errors.* :
                if the query is not correct due to syntax or wrong names.
        '''
        new_cursor = self.__new_cursor()
        new_cursor.itersize = self.__batch_size
        new_cursor.execute("SELECT data_json as json FROM {} WHERE {};".format(
            self.__query.category, self.__query.filters))
        return _PostgreSQLIterator(new_cursor)

    def __new_cursor(self):
        '''
        Returns:
            A new cursor with a guaranteed unique name for this stream.
        '''
        name = PostgreSQLStream.__CURSOR_NAME.format(
            PostgreSQLStream.__CURSOR_ID)
        PostgreSQLStream.__CURSOR_ID += 1
        return self.__conn.cursor(name)
