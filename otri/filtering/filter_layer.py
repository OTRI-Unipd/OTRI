from .filter import Filter, Collection, Iterable

class FilterLayer:
    '''
    Defines a list of filters that could be executed in parallel.
    '''

    def __init__(self, filters : Collection[Filter]):
        '''
        Initializes the layer.

        Parameters:
            filters : Collection[Filter]
                Filters that use input streams from other layers (that should be executed before this one).
        '''
        self.filters = filters

    def __getitem__(self, index : int)->Filter:
        return self.filters[index]
    
    def __len__(self):
        return len(self.filters)

    def __iter__(self):
        return self.filters.__iter__()

    def get_filters(self)->Collection[Filter]:
        '''
        Retrieve the filters list.
        '''
        return self.filters