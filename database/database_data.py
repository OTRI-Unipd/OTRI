class DatabaseData:
    '''
    Representation of data read or to write in a no-sql database

    Attributes:
        category : str
            Which document or root element this data belongs to
        values : dict
            Data to insert into the document or under the root element
    '''
    def __init__(self, category : str, values):
        '''
        Parameters:
            category : str
                What category/document/root this data is from
            values : dict or list
                Data read from the category
        '''
        self.category = category
        self.values = values