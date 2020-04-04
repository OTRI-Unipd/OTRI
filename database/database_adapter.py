from database.database_query import DatabaseQuery
from database.database_data import DatabaseData

'''
Classes:
DatabaseAdapter -- Abstract class
'''
class DatabaseAdapter:
    '''
        Abstract class used to access with the same methods independently from the kind of database used.
    '''
    def write(self, data : DatabaseData):
        '''
        Writes data.values inside the database in the rigth data.category
        '''
        raise NotImplementedError("This is an abstract method, please implement it in a sub-class")

    def read(self, query : DatabaseQuery):
        '''
        Reads all values from 
        '''
        raise NotImplementedError("This is an abstract method, please implement it in a sub-class")