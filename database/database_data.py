class DatabaseData:
    '''
    Representation of data read or to write in a no-sql database

    Parameters:
        category : str (?)
            Which document or root element this data belongs to
        values : dict
            Data to insert into the document or under the root element
    '''
    def __init__(self, category : str, values : dict):
        '''
        Arguments:
            category : str
                What category/document/root this data is from
            values : dict
                Data read from the category
        '''
        self.category = category
        self.values = values

    def get_category(self):
        '''
        Returns data category as str
        '''
        return self.category
    
    def get_values(self):
        '''
        Returns values as dict
        '''
        return self.values