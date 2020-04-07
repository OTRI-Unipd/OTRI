class DatabaseQuery:
    '''
    Representation of a database query to read data.

    Parameters:
        category : str
            Document or collection where to read data.
        filters : dict
            Select only rows with these filters satisfied.
    '''
    def __init__(self, category : str, filters : dict):
        '''
        Arguments:
            category : str
                What category/document/root look for data in
            filters : dict
                Data filters in form of key-value
        '''
        self.category = category
        self.filters = filters