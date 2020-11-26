
from typing import Any, Sequence

from ..filtering.queue import Queue


class Analysis:
    '''
    Superclass for analysis.
    '''

    def execute(self, in_queues: Sequence[Queue]) -> Any:
        '''
        Starts data analyis.\n

        Parameters:\n
            in_queues : Queue
                Any kind of queue required by the analysis.\n
        Returns:
            Results, depends on the type of analysis.\n
        '''
        raise NotImplementedError("This is an abstract class")
