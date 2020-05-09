from .filter import Filter, Collection, Iterator

class FilterLayer(list):
    '''
    Defines a list of filters that could be executed in parallel.
    '''

    def is_finished(self):
        '''
        Retruns whether all the filters in the layer are flagged as finished.
        '''
        for filter in super().__iter__():
            if not filter.is_finished():
                return False
        return True