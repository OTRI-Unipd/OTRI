from .filter import Filter, Collection, Iterator

class FilterLayer(list):
    '''
    Defines a list of filters that could be executed in parallel.
    '''

    def is_finished(self):
        '''
        Retruns whether allt he filters in this layer are flagged 'is_finished'.
        '''
        for filter in super().__iter__():
            if not filter.is_finished:
                return False
        return True