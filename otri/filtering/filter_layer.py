

class FilterLayer(list):
    '''
    Defines a list of filters that could be executed in parallel.
    '''

    def is_finished(self) -> bool:
        '''
        Retruns whether all the filters in the layer are flagged as finished.
        '''
        for fil in super().__iter__():
            if not fil.is_finished():
                return False
        return True
