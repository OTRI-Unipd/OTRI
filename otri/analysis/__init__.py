
from ..filtering.stream import Stream
from typing import Sequence


class Analysis:
    '''
    Superclass for analysis.
    '''

    def execute(self, in_streams: Sequence[Stream]):
        '''
        Starts data analyis.\n

        Parameters:\n
            in_streams : Stream
                Any kind of stream required by the analysis.
        '''
        raise NotImplementedError("This is an abstract class")
