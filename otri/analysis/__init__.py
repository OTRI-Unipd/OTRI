
from typing import Any, Sequence

from ..filtering.stream import Stream


class Analysis:
    '''
    Superclass for analysis.
    '''

    def execute(self, in_streams: Sequence[Stream]) -> Any:
        '''
        Starts data analyis.\n

        Parameters:\n
            in_streams : Stream
                Any kind of stream required by the analysis.\n
        Returns:
            Results, depends on the type of analysis.\n
        '''
        raise NotImplementedError("This is an abstract class")
