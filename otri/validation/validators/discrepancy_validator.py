__author__ = "Riccardo De Zen <riccardodezen98@gmail.com>"
__version__ = "1.0"

from .. import ParallelValidator
from ..exceptions import DiscrepancyError

from typing import TypeVar, Sequence, Mapping, List

K = TypeVar('K')
'''Generic atom key type.'''


class DiscrepancyValidator(ParallelValidator):

    '''
    Validator aiming to find discrepancies between two or more Streams.
    The Streams are checked two by two, and if their values differ by more than a certain level,
    their atoms are marked as having a high discrepancy.

    The Streams can be more than two, and the discrepancy is applied from left to right.
    Example: given the Streams A,B and C. Maximum discrepancy level is 10%. Notice discrepancy
    limits are inclusive.

    - Check that `a * 0.9 <= b <= a * 1.1 for each a in A and b in B`.
    - Check that `a * 0.9 <= c <= a * 1.1 for each c in A and c in C`.
    - Check that `b * 0.9 <= c <= b * 1.1 for each b in B and c in C`.

    In order to produce results that make sense, you may want to interpolate the atoms to fill in
    order to have Streams that are parallel in the "datetime" axis. If interpolating does not suit
    your needs, you still may want to check for gaps. See `ContinuityValidator` for that.
    '''

    def __init__(self, inputs: Sequence[str], outputs: Sequence[str], limits: Mapping[K, float]):
        '''
        This Validator expects the same number of Stream inputs and outputs.

        Parameters:
            inputs : str
                Names of the inputs.\n
            outputs : str
                Names of the outputs.\n
            limits : Mapping
                Mapping of keys to their discrepancy limits, expressed as a float.
                No checks are performed on the limits' sense, **you have been warned**.
                Any limit is allowed, the expression is always:

                for k, limit in self._limits.items():
                    if not (first[k] * (1 - limit) <= second[k] <= first[k] * (1 + limit)):
                        # DiscrepancyError...
        '''
        super().__init__(inputs, outputs)
        self._limits = limits

    def _check(self, data: List[Mapping], indexes: List[int]):
        '''
        For each atom in data check the discrepancy with all the successive atoms.

        Parameters:
            data : List[Mapping]
                The atoms retrieved from the inputs.

            indexes : List[int]
                The indexes from which the respective atoms come from.
        '''
        # No discrepancy to be found if single Stream is left open.
        if len(data) == 1:
            return

        # all but the last atom.
        for i, atom in enumerate(data[:-1:]):
            for j, other in enumerate(data[i + 1::]):
                self._discrepancy(atom, other, indexes[i], indexes[i + 1 + j])

    def _discrepancy(self, first: Mapping, second: Mapping, first_index: int, second_index: int):
        '''
        Compare two atoms and see if they are further apart from the given limit for each given key.

        Parameters:
            first : Mapping
                The first of the two atoms.

            second : Mapping
                The second atom.

            first_index : int
                The index of the input `Stream` from which `first` comes.

            second_index : int
                The index of the input `Stream` from which `second` comes.
        '''
        for k, limit in self._limits.items():
            if not (first[k] * (1 - limit) <= second[k] <= first[k] * (1 + limit)):
                error = DiscrepancyError(
                    limit, {str(first_index): first[k], str(second_index): second[k]}
                )
                self._add_label(first, error)
                self._add_label(second, error)
