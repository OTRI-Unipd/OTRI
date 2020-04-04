class DatabaseData:
    '''
    Representation of data read or to write in a no-sql database

    Parameters:
        category
            Which document or root element this data belongs to
        values : dict
            Data to insert into the document or under the root element
    '''
    def __init__(self, category : str, values : dict):
        self.category = category
        self.values = values

    def get_category(self):
        return self.category
    
    def get_values(self):
        return self.values