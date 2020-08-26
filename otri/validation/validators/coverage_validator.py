from .. import MonoValidator
# from ..exceptions import CoverageError

from typing import Mapping, Any, Iterable


class CoverageValidator(MonoValidator):

    '''
    Ensures values cover a whole certain interval without gaps, if gaps are found, it fills them in
    with empty atoms consisting of only the keys where the values are missing, such values and a
    `CoverageError`.

    The intervals must be sorted, and the input Stream is assumed to be already sorted on that value
    itself. This is so that this Validator can fill in the gaps it finds properly, by stopping when
    the expected values reach the value in the Stream. Also, the input Stream should not contain
    values that are not in the interval to cover, example:

    stream = [0...10, 12, 13...]
    interv = [0...10, 11, 13...]

    After 10, the Validator encounters a gap because 11 is not found, and since 12 never appears in
    the interval, the gap is assumed to span for the whole remaining interval.
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
                end of the Stream.
        '''
        super().__init__(inputs, outputs)
        # Cache the iterators to use by calling "next".
        self._intervals = {k: iter(interval) for k, interval in intervals.items()}

    def _check(self, data: Mapping):
        '''
        Ensures the the atom contains the expected values, if not, inject atoms in the Stream until
        all the values are the expected ones again.

        Parameters:
            data : Mapping
                The atom to check.
        '''
        raise NotImplementedError("Halp")
