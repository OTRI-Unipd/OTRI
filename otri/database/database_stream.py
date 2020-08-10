from typing import Tuple


class DatabaseIterator:
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


class DatabaseStream:

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

    def __init__(self, cursor, parent):
        '''
        Parameters:
            cursor
                A cursor on which the query to iterate through has already been executed without fail.
            parent : PostgreSQLStream
                The parent Stream. Will be closed when the stream is over and the cursor closed.
        '''
        super().__init__()
        self.__cursor = cursor
        self.__parent = parent
        self.__buffer = None

    def __next__(self):
        '''
        Returns:
            The next element in the sequence, fecthing it from the database.
        Except:
            StopIteration
                When no further element can be retrieved.
        '''
        if self.__cursor.closed:
            raise StopIteration
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

    def has_next(self) -> bool:
        '''
        Returns:
            True if there is a next element.
            False if there is None
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
        Closes the parent Stream.
        '''
        if not self.__cursor.closed:
            self.__cursor.close()
        if not self.__parent.is_closed():
            self.__parent.close()


class PostgreSQLStream(DatabaseStream):

    __CURSOR_NAME = "otri_cursor_{}"
    __CURSOR_ID = 0

    def __init__(self, connection, query: str, batch_size: int = 1000):
        '''
        Parameters:\n
            connection : psycopg2.connection\n
                A connection to the desired database.\n
            query : DatabaseQuery\n
                The query to stream.\n
            batch_size : int = 1000\n
                The amount of rows to fetch each time the cached rows are read.\n
        Raises:
            psycopg2.errors.* :\n
                if the query is not correct due to syntax or wrong names.
        '''
        super().__init__()
        self.__connection = connection
        self.__batch_size = batch_size
        self.__cursor = self.__new_cursor(connection)
        self.__cursor.execute(query)
        self.__iter = _PostgreSQLIterator(self.__cursor, self)
        self.__is_closed = False

    def __iter__(self) -> _PostgreSQLIterator:
        '''
        Returns:
            The iterator for this object.
        '''
        return self.__iter

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
        if self.__is_closed:
            raise RuntimeError("cannot flag stream as closed twice")
        self.__is_closed = True
        self.__connection.close()

    def __new_cursor(self, connection):
        '''
        Parameters:\n
            connection\n
                The connection from which to create the cursor.\n
        Returns:\n
            A new cursor with a guaranteed unique name for this stream.
        '''
        name = PostgreSQLStream.__CURSOR_NAME.format(
            PostgreSQLStream.__CURSOR_ID)
        PostgreSQLStream.__CURSOR_ID += 1
        cursor = connection.cursor(name)
        cursor.itersize = self.__batch_size
        return cursor
