from typing import Sequence, Callable, Mapping, List, Any, Set, Tuple, Final
from ..filtering.filter import Filter
from ..filtering.stream import Stream


class ValidatorFilter(Filter):

    '''
    This Filter is used to apply a check to a list of atoms.
    This is an abstract class and should be further extended implementing the `_check(atom)` method.
    '''

    OK: Final = -1
    UNCLEAR: Final = 0
    ERROR: Final = 1
    WARNING: Final = 2
    ERR_KEY: Final = "error"
    WARN_KEY: Final = "warning"

    def _on_data(self, data, index):
        '''
        Called when input data is found.

        Parameters:
            data : Any
                Popped data from an input.
            index : int
                The index of the input the data has been popped from.
        Returns:
            The result of the check on the data (to allow extension with more cases).
        '''
        result = self._check(data)
        ValidatorFilter.__ACTIONS[result](self, data)
        return result

    # ? MANDATORY OVERRIDE ---

    def _check(self, data) -> Tuple[int, str]:
        '''
        Check a single atom. Must be implemented.
        Should return one of `ValidatorFilter.OK`, `ValidatorFilter.UNCLEAR`, `ValidatorFilter.WARNING`
        or `ValidatorFilter.ERROR` and an informative optional message.

        Parameters:
            data
                The data to check.
        '''
        raise NotImplementedError("ValidatorFilter is an abstract class, please extend it.")

    # ? OPTIONAL OVERRIDE ---

    def _on_ok(self, data):
        '''
        Called if data resulted ok.

        Parameters:
            data
                The checked data.
        '''
        pass

    def _on_unclear(self, data):
        '''
        Called if data resulted ok.

        Parameters:
            data
                The checked data.
        '''
        pass

    def _on_error(self, data):
        '''
        Called if data resulted ok.

        Parameters:
            data
                The checked data.
        '''
        pass

    def _on_warning(self, data):
        '''
        Called if data resulted ok.

        Parameters:
            data
                The checked data.
        '''
        pass

    # ? STATIC METHODS ---

    @staticmethod
    def __add_label(atom, key, value):
        '''
        Add an error or warning label to an atom.
        Add an empty `list` in `atom` for `key` if `key` is not present, then append `value`.
        '''
        if key not in atom.keys():
            atom[key] = list()
        atom[key].append(value)

    # Mapping methods to avoid if-else chain.
    # Needs to be done after methods declaration.
    __ACTIONS = {
        OK: _on_ok,
        UNCLEAR: _on_unclear,
        ERROR: _on_error,
        WARNING: _on_warning
    }


class MonoValidator(ValidatorFilter):

    '''
    This class extends ValidatorFilter and is meant to be used when running checks on single atoms
    from one Stream, outputting them to another single Stream.
    '''

    pass


class BufferedValidator(ValidatorFilter):

    '''
    This class extends ValidatorFilter and is meant to be used when running checks on a larger group
    of atoms, but still expects a single input and output.
    '''

    pass


class MultiValidator(ValidatorFilter):

    '''
    This class extends ValidatorFilter and is meant to run checks on multiple Streams at the same
    time (i.e. discrepancy between parallel Streams).
    '''

    # Should probs override _input_check_order(...)

    pass