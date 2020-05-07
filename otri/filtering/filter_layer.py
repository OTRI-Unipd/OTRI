from .filter import Filter, Collection, Iterator

class FilterLayer(list):
    '''
    Defines a list of filters that could be executed in parallel.
    '''

    def is_finished(self):
        '''
        Retruns whether all the output of the filters in this layer are flagged as 'is_finished'.
        '''
        for filter in super().__iter__():
            for output in filter.get_output_streams():
                if not output.is_finished():
                    #print("output not finished")
                    return False
        return True