import psycopg2

from .database_query import DatabaseQuery


class DatabaseStream():

    def __iter__(self):
        '''
        Returns:
            The iterator for this object.
            Can be its own iterator.
        '''
        pass

    def __next__(self):
        '''
        Returns:
            The next element in the sequence, fecthing it from the database.
        Except:
            StopIteration
                When no result is given by the database (the query is over).
        '''
        pass

    def has_next(self):
        '''
        Returns:
            True - if the stream has a next item.
            False - if the stream has no other item.
        '''
        pass


class PostgreSQLStream(DatabaseStream):

    def __init__(self, cursor, query: DatabaseQuery):
        '''
        Parameters:
            cursor : psycopg2.cursor
                A named cursor that can stream the given query.
            query : DatabaseQuery
                The query to stream.
        '''
        super().__init__()
        self.__cursor = cursor
        self.__query = query

    def __iter__(self):
        '''
        Returns:
            The iterator for this object.
            Can be its own iterator.
        '''
        self.__cursor.execute("SELECT data_json as json FROM {} WHERE {};".format(
            self.__query.category, self.__query.filters))
        return self

    def __next__(self):
        '''
        Returns:
            The next element in the sequence, fecthing it from the database.
        Except:
            StopIteration
                When no result is given by the database (the query is over).
        '''
        try:
            nx = next(self.__cursor)
        except StopIteration:
            self.__cursor.close()
            raise StopIteration

    def has_next(self):
        return not self.__cursor.closed

    def close(self):
        '''
        Closes the cursor.
        '''
        self.__cursor.close()