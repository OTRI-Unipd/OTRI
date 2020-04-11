class DatabaseQuery:
    '''
    Representation of a database query to read data.

    Attributes:
        category : str
            Document or collection where to read data.
        filters : dict
            Select only rows with these filters satisfied.
    '''
    def __init__(self, category : str, filters : str):
        '''
        Parameters:
            category : str
                What category/document/root look for data in
            filters : dict
                Query contitions (ex "json->'ticker' = 'AAPL'")
        '''
        self.category = category
        self.filters = filters