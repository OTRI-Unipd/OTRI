

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

    @staticmethod
    def EXEC_ALL(layer):
        for fil in layer:
            if not fil.is_finished():
                return 0
        return +1

    @staticmethod
    def EXEC_ONE_AND_PASS(layer):
        return +1

    @staticmethod
    def BACK_IF_NO_OUTPUT(layer):
        for fil in layer:
            if fil.has_output_anything():
                return 0
            elif not fil.is_finished():
                return -1
        return +1

    @staticmethod
    def EXEC_UNTIL_OUTPUT(layer):
        for fil in layer:
            if fil.has_output_anything():
                return +1
        return 0
