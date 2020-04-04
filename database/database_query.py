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
        self.category = category
        self.filters = filters