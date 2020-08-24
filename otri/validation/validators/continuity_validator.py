__author__ = "Riccardo De Zen <riccardodezen98@gmail.com>"
__version__ = "1.0"

from .. import MonoValidator
from ..exceptions import ContinuityError

from typing import Callable, Mapping


class ContinuityValidator(MonoValidator):

    '''
    Single Stream validator class aiming to check whether the values in a Stream are contiguous.
    The exact meaning of this depends on the implementation, but in general this is what the filter
    does:

    1. Retrieve an atom.
    2. If no atom has been seen before, release it, keeping a reference, and retrieve another one.
    3. Check wether the two atoms are _contiguous_, based on the implementation.
    4. If they are NOT, mark them both with the thrown exception.
    5. Replace the reference to the first atom with one to the second and release it.
    6. Repeat from step one.
    '''

    def __init__(self, inputs, outputs, key, continuity: Callable):
        '''
        Parameters:
            inputs : str
                Name for the single input stream.\n
            outputs : str
                Name for the single output stream.\n
            key : str
                The key for which to check whether the values are continuous.\n
            continuity : Callable
                The method or function defining the concept of continuity between two values.
                Must take the two values (first, second) as parameters and return a boolean.
        '''
        super().__init__(inputs, outputs)
        self._key = key
        self._continuity = continuity
        self._last_atom = None

    def _check(self, data: Mapping):
        '''
        Check if two atoms are contiguous.

        Parameters:
            data : Mapping
                The data to check.
        Raises:
            Will raise an Exception if there is some problem in the atom.
        '''
        last = self._last_atom
        key = self._key
        if last is not None:
            first = last[key]
            second = data[key]
            if not self._continuity(first, second):
                # Mark the other atom too.
                error = ContinuityError({key: [first, second]})
                self._add_label(self._last_atom, error)
                self._last_atom = data
                raise error
        # Cache this atom for later, but still release it.
        self._last_atom = data
