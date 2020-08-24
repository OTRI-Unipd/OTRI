__author__ = "Riccardo De Zen <riccardodezen98@gmail.com>"
__version__ = "1.0"

from .. import BufferedValidator
from ..exceptions import ClusterWarning

from typing import Mapping


class ClusterValidator(BufferedValidator):
    '''
    Verify that no clusters bigger than N are formed for a set of keys.

    The Validator always holds atoms until it has determined that there is or there is not a cluster
    big enough to trigger a warning.
    '''

    def __init__(self, inputs: str, outputs: str, key: str, limit: int):
        '''
        Parameters:
            inputs : str
                Name for the single input stream.\n
            outputs : str
                Name for the single output stream.\n
            key : str
                The key to monitor for clusters.\n
            limit: str
                Any cluster strictly higher than limit will be marked.
        '''
        # Single input and output super constructor call.
        super().__init__(inputs, outputs)

        self._cluster_key = key
        self._cluster_limit = limit
        self._cluster_size = 0

        # Hold by default.
        self._holding = True

    def _check(self, data: Mapping):
        '''
        Check a single atom.
        - If the internal buffer is empty it won't do anything.
        - If the internal buffer is not empty and the value of the top atom in the buffer is equal
        to the current atom's value, increase the cluster size.
        - If the internal buffer is not empty and the value of the top atom in the buffer is different
        than the current atom's value, look at the cluster size, if it's higher than the given limit,
        append a ClusterWarning on all of the buffer's atoms and release them. Reset the cluster size.

        Parameters:
            data : Mapping
                The data to check.
        Raises:
            Will raise an Exception if it found.
        '''
        # Empty buffer, do nothing.
        if not self._hold_buffer:
            self._cluster_size = 1
            return

        if self._buffer_top()[self._cluster_key] == data[self._cluster_key]:
            self._cluster_size += 1
        else:
            self._check_cluster()

    def _check_cluster(self):
        '''
        When a cluster is found:
        - Mark all the atoms in the buffer
        - Release them
        - Reset cluster size
        '''
        if self._cluster_size > self._cluster_limit:
            self._error_all(ClusterWarning({self._cluster_key, self._cluster_size}))
        # Either way reset cluster.
        self._release()
        self._holding = True
        self._cluster_size = 1

    def _on_inputs_closed(self):
        '''
        All of the inputs are closed and no more data is available.
        The filter empties the buffer and closes its outputs.
        '''
        self._check_cluster()
        super()._on_inputs_closed()
