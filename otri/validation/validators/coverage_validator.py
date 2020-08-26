__author__ = "Riccardo De Zen <riccardodezen98@gmail.com>"
__version__ = "1.0"

from .. import MonoValidator
from ..exceptions import CoverageError

from typing import Mapping, Any, Iterable


class CoverageValidator(MonoValidator):

    '''
    Ensures values cover a whole certain interval without gaps, if gaps are found, it fills them in
    with empty atoms consisting of only the keys where the values are missing, such values and a
    `CoverageError`. If an interval finishes yielding elements before the Stream ends, the interval
    is removed and any value is accepted on that key from that point onwards, hence some kind of
    endless generator is recommended.

    Give an interval to cover I, and a Stream S. If S[i] != I[i], then the Filter keeps injecting
    atoms until S[i] == I[j] for some j > i.
    Example:

    stream = [0...10, 12, 13...]
    interv = [0...10, 11, 13...]

    After 10, the Validator encounters a gap because 11 is not found, and since 12 never appears in
    the interval, the gap is assumed to span for the whole remaining interval.

    If more than one interval is supplied, the values must match for all the keys in order for an
    atom to proceed.
    '''

    def __init__(self, inputs: str, outputs: str, intervals: Mapping[Any, Iterable]):
        '''
        Parameters:
            inputs : str
                Name for the single input stream.

            outputs : str
                Name for the single output stream.

            intervals : Mapping[Any, Iterable]
                Mapping of each key to check to the values it should have, from the beginning to the
                end of the Stream. Generator heavily suggested. Stream won't work unless already
                closed.
        '''
        super().__init__(inputs, outputs)
        # Cache the iterators to use by calling "next".
        self._intervals = {k: iter(interval) for k, interval in intervals.items()}

    def _check(self, data: Mapping):
        '''
        Ensures the the atom contains the expected values, if not, inject atoms in the Stream until
        all the values are the expected ones again and `data` can be let through.
        If an interval runs out it is removed and will not be checked again.

        Parameters:
            data : Mapping
                The atom to check.
        '''
        while True:
            # missing_values will serve as a fake atom.
            missing_values = dict()
            empty_intervals = set()

            for key, interval in self._intervals.items():
                try:
                    next_value = next(interval)
                except StopIteration:
                    # Interval ran out, any value is ok.
                    empty_intervals.add(key)
                    continue
                if data[key] != next_value:
                    missing_values[key] = next_value

            for key in empty_intervals:
                self._intervals.pop(key)

            if not missing_values:
                break

            error = CoverageError(missing_values)
            # Flag it.
            self._add_label(missing_values, error)
            # Push it.
            self._push_data(missing_values)
