from typing import Sequence, Callable, Mapping, List, Any, Set, Tuple, Final
from ..filtering.filter import Filter
from ..filtering.stream import Stream
from datetime import datetime


class ValidatorFilter(Filter):

    OK: Final = 0
    ERROR: Final = 1
    WARNING: Final = 2
    ERR_KEY: Final = "error"
    WARN_KEY: Final = "warning"

    '''
    This is a single input, single output Filter used to apply some checks to a Stream of atoms.
    Each "check" is a Callable of type `Callable[[Mapping, List], Tuple[int, str]]` that receives
    the atom to check and its `neighbors`, which are a `list` made available to each check by the
    Filter where the check can store atoms.
    
    # TODO When a check returns an error or warning, those are applied to an atom and all of its neighbors.
    
    The given Callables should return either `ValidatorFilter.OK`, `ValidatorFilter.ERROR` or
    `ValidatorFilter.WARNING`, the last two cases can provide an optional informative message.
    The two outputs have to be in a Tuple.
    Every time a warning or error is returned, a tuple containing the check and the message is
    added to the "warning" or "error" fields in the atom. These fields are created if they do not
    exist.
    '''

    def __init__(self, inputs: str, outputs: str, checks: Set[Callable[[Mapping, List], Tuple[int, str]]]):
        '''
        Parameters:
            inputs : str
                The name of the input Stream
            outputs : str
                The name of the output Stream
            checks : Set[Callable]
                The set of Checks this Filter should perform on the atoms that pass through.
        '''
        super().__init__([inputs], [outputs], input_count=1, output_count=1)
        self.__checks = checks

    def setup(self, inputs: Sequence[Stream], outputs: Sequence[Stream], state: Mapping[str, Any]):
        '''
        Used to save references to streams and reset variables.
        Called once before the start of the execution in FilterList.

        Parameters:
            inputs, outputs : Sequence[Stream]
                Ordered sequence containing the required input/output streams gained from the FilterList.
            state : Mapping[str, Any]
                Dictionary containing states to keep.
        '''
        self.__input = inputs[0]
        self.__input_iter = iter(inputs[0])
        self.__output = outputs[0]
        self.__state = state
        # `neighbors`
        for check in self.__checks:
            state[check] = list()

    def execute(self):
        '''
        Runs all the checks on a given atom.
        '''
        if self.__output.is_closed():
            return
        if self.__input_iter.has_next():
            atom = next(self.__input_iter)
            for check in self.__checks:
                self.__eval(atom, check)
            self.__output.append(atom)
        # Check that we didn't just pop the last item
        elif self.__input.is_closed():
            self.__output.close()

    def __eval(self, atom: Mapping, check: Callable[[Mapping, List], Tuple[int, str]]):
        '''
        Pass a single atom through a check and add warn/errors to it if due.
        Parameters:
            atom : Mapping
                The atom to evaluate.
            check : Callable
                The check that needs to be evaluated.
        '''
        error_key = ValidatorFilter.ERR_KEY
        warning_key = ValidatorFilter.WARN_KEY

        result, message = check(atom, self.__state[check])
        if result == ValidatorFilter.WARNING:
            ValidatorFilter.__add_label(atom, warning_key, (check, message))
            # ? TODO Apply to all the neighbors
        elif result == ValidatorFilter.ERROR:
            ValidatorFilter.__add_label(atom, error_key, (check, message))
            # ? TODO Apply to all the neighbors

    @staticmethod
    def __add_label(atom, key, value):
        '''
        Add an error or warning label to an atom.
        Add an empty `list` in `atom` for `key` if `key` is not present, then append `value`.
        '''
        if key not in atom.keys():
            atom[key] = list()
        atom[key].append(value)


def make_check_date_between(date1: datetime, date2: datetime, inclusive: bool = False) -> Callable[[datetime, Mapping], Tuple[int, str]]:
    '''
    Parameters:
        date1 : datetime
            The first date
        date2 : datetime
            The second date
        inclusive : bool = False
            Whether to include the two dates in the accepted interval

    Returns:
        Callable[datetime] : Checks whether the given datetime is in-between the two given dates.
        Will choose the smallest date as start date
    '''
    start = min([date1, date2])
    end = max([date1, date2])
    inclusive = inclusive

    def check_date_between(d: datetime, state: Mapping = None) -> Tuple[bool, str]:
        '''
        Parameters:
            d : datetime
                A date to check
            state : Mapping
                State for this check, unused.
        Returns:
            `ValidatorFilter.OK` and None if parameter d is after start and before end.
            `ValidatorFilter.ERROR` and a string error if it isn't. If inclusive is False also
            does not consider `start` and `end` as "between".
            Does NOT check timezones.
        '''
        if inclusive:
            if start <= d and d <= end:
                return ValidatorFilter.OK, None
            else:
                return ValidatorFilter.ERROR,
                "Date not between {} and {} (inclusive).".format(start, end)
        else:
            if start < d and d < end:
                return ValidatorFilter.OK, None
            else:
                return ValidatorFilter.ERROR,
                "Date not between {} and {} (non-inclusive).".format(start, end)

    return check_date_between
