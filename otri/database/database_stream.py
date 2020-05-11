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
        raise NotImplementedError(
            "DatabaseIterator is an abstract class, please implement this method in a subclass"
        )

    def has_next(self) -> bool:
        '''
        Returns:
            True - if the stream has a next item.
            False - if the stream has no other item.
        '''
        raise NotImplementedError(
            "DatabaseIterator is an abstract class, please implement this method in a subclass"
        )


class DatabaseStream():

    def __iter__(self) -> DatabaseIterator:
        '''
        Returns:
            The iterator for this object, returns a __DatabaseIterator object
        '''
        raise NotImplementedError(
            "DatabaseStream is an abstract class, please implement this method in a subclass"
        )

    def is_closed(self) -> bool:
        '''
        Defines if new data might be added to the stream.
        '''
        raise NotImplementedError(
            "DatabaseStream is an abstract class, please implement this method in a subclass"
        )

    def close(self):
        '''
        Prevents the stream from getting new data, data contained can still be iterated.
        '''
        raise NotImplementedError(
            "DatabaseStream is an abstract class, please implement this method in a subclass"
        )


class _PostgreSQLIterator(DatabaseIterator):

    def __init__(self, cursor):
        '''
        Parameters:
            cursor
                A cursor on which the query to iterate through has already been executed without fail.
        '''
        super().__init__()
        self.__cursor = cursor
        self.__buffer = None

    def __next__(self):
        '''
        Returns:
            The next element in the sequence, fecthing it from the database.
        Except:
            StopIteration
                When no result is given by the database (the query is over),
                closes the cursor and raises StopIteration again.
        '''
        if self.__buffer != None:
            item = self.__buffer
            self.__buffer = None
            return item
        else:
            return next(self.__cursor)[0]

    def has_next(self) -> bool:
        '''
        Returns:
            True if the cursor is still open (there are still rows to retrieve)
            False if the cursor is closed (the query is over)
        '''
        if self.__buffer != None:
            return True
        try:
            self.__buffer = next(self.__cursor)[0]
            return True
        except StopIteration:
            self.__cursor.close()
            return False

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
            connection : psycopg2.connection
                A connection to the desired database.
            query : DatabaseQuery
                The query to stream.
            batch_size : int = 1000
                The amount of rows to fetch each time the cached rows are read.
        '''
        super().__init__()
        self.__batch_size = batch_size
        self.__cursor = self.__new_cursor(connection)
        self.__cursor.execute("SELECT data_json as json FROM {} WHERE {};".format(
            query.category, query.filters)
        )
        self.__is_closed = False

    def __iter__(self) -> _PostgreSQLIterator:
        '''
        Returns:
            The iterator for this object.
            Can be its own iterator.
        Raises:
            psycopg2.errors.* :
                if the query is not correct due to syntax or wrong names.
        '''
        return _PostgreSQLIterator(self.__cursor)

    def is_closed(self) -> bool:
        '''
        Defines if new data might be added to the stream.

        Returns:
            True if the stream has been closed, False otherwise.
        '''
        return self.__is_closed

    def close(self):
        '''
        Prevents the stream from getting new data, data contained can still be iterated.
        '''
        self.__is_closed = True

    def __new_cursor(self, connection):
        '''
        Parameters:
            connection
                The connection from which to create the cursor.
        Returns:
            A new cursor with a guaranteed unique name for this stream.
        '''
        name = PostgreSQLStream.__CURSOR_NAME.format(
            PostgreSQLStream.__CURSOR_ID)
        PostgreSQLStream.__CURSOR_ID += 1
        cursor = connection.cursor(name)
        cursor.itersize = self.__batch_size
        return cursor
