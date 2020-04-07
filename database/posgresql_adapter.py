from database.database_adapter import DatabaseAdapter, DatabaseData, DatabaseQuery

class PosgreSQLAdapter(DatabaseAdapter):
    '''
    Database adapter for postgreSQL
    '''

    def write(self, data : DatabaseData):
        '''
        Writes data.values inside the database in the data.category (could be a table ? or a row ?)

        Parameters:
            data : DatabaseData
                Data to write in DB
        '''
        print("Write")
    
    def read(self, query : DatabaseQuery):
        '''
        Queries the database for the requested values.
        TODO: Define how a postgre JSON query should be formatted

        Parameters:
            query : DatabaseQuery
                Executes the query on the given query.category and the given query.filters
        Returns:
            dict containing queried data
        '''
        print("read")
        return dict()