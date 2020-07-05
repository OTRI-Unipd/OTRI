from typing import Sequence, Callable, Mapping, List, Any, Set, Tuple, Final
from ..filtering.filter import Filter
from ..filtering.stream import Stream


class ValidatorFilter(Filter):

    '''
    This Filter is used to apply a check to a list of atoms.
    This is an abstract class and should be further extended implementing the `_check(atom)` method.

    The `_check(atom)` method should return one of the 4 class constants (OK, UNCLEAR, ERROR and 
    WARNING). Plus a message. Messages in case of OK and UNCLEAR are ignored by default, so they
    can be `None`, but still must be returned.

    The constants in this class are meant to be used as follows:
    - OK: The atom has no errors, should not be modified. No message is needed.
    - UNCLEAR: The atom may or may not have errors, depending on the values of its neighbors. This
    error code should be returned only on ValidatorFilters that keep an atom buffer. No message is
    needed.
    - ERROR and WARNING: The check found an error or a warning in the atom. The given message and
    Filter name should be appended to the atom's ERR_KEY (or WARN_KEY respectively) list field.
    '''

    OK: Final = -1
    UNCLEAR: Final = 0
    ERROR: Final = 1
    WARNING: Final = 2
    ERR_KEY: Final = "error"
    WARN_KEY: Final = "warning"

    def _on_data(self, data: Mapping, index):
        '''
        Called when input data is found.

        Parameters:
            data : Mapping
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

    def _check(self, data: Mapping) -> Tuple[int, Any]:
        '''
        Check a single atom. Must be implemented.
        Should return one of `ValidatorFilter.OK`, `ValidatorFilter.UNCLEAR`, `ValidatorFilter.WARNING`
        or `ValidatorFilter.ERROR` and an informative optional message.

        The informative message should be any human readable format, like a string, but any type is
        accepted by default.

        Parameters:
            data : Mapping
                The data to check.
        '''
        raise NotImplementedError("ValidatorFilter is an abstract class, please extend it.")

    # ? OPTIONAL OVERRIDE ---

    def _on_ok(self, data: Mapping):
        '''
        Called if data resulted ok.

        Parameters:
            data : Mapping
                The checked data.
        '''
        pass

    def _on_unclear(self, data: Mapping):
        '''
        Called if data resulted unclear.

        Parameters:
            data : Mapping
                The checked data.
        '''
        pass

    def _on_error(self, data: Mapping, msg):
        '''
        Called if data has some error.

        Parameters:
            data : Mapping
                The checked data.

        '''
        pass

    def _on_warning(self, data: Mapping, msg):
        '''
        Called if data has a warning.

        Parameters:
            data : Mapping
                The checked data.
            msg
                The warning message.
        '''
        pass

    # ? CLASS METHODS ---

    @classmethod
    def _add_label(cls, atom: Mapping, key, value):
        '''
        Helper method to add an error or warning label to an atom.
        Add an empty `list` in `atom` for `key` if `key` is not present.
        Append `value` to the `key` field in `atom`.

        Parameters:
            atom : Mapping
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

    This class does not keep an internal buffer for "uncertain" atoms, so `_check()` should never
    return `ValidatorFilter.UNCLEAR`. Refer to `BufferedValidator` for a buffered alternative.

    This class is fully operational by overriding `_check()` alone.
    '''

    def __init__(self, inputs: str, outputs: str):
        '''
        Parameters:
            inputs : str
                Name for input stream.
            outputs : str
                Name for output stream.
        '''
        # Single input and output super constructor call.
        super().__init__([inputs], [outputs], 1, 1)

    def _on_ok(self, data: Mapping):
        '''
        Called if data resulted ok.
        Pushes the atom to the output.

        Parameters:
            data : Mapping
                The checked data.
        '''
        self._push_data(data)

    def _on_error(self, data: Mapping, msg):
        '''
        Called if data has some error.
        Appends the error message to the `ValidatorFilter.ERR_KEY` field in the atom then pushes it
        to the output.

        Parameters:
            data : Mapping
                The checked data.
        '''
        self._add_label(data, ValidatorFilter.ERR_KEY, msg)
        self._push_data(data)

    def _on_warning(self, data: Mapping, msg):
        '''
        Called if data has a warning.
        Appends the warning message to the `ValidatorFilter.WARN_KEY` field in the atom then pushes
        it to the output.

        Parameters:
            data : Mapping
                The checked data.
        '''
        self._add_label(data, ValidatorFilter.WARN_KEY, msg)
        self._push_data(data)


class BufferedValidator(ValidatorFilter):

    '''
    This class extends ValidatorFilter and is meant to be used when running checks on a larger group
    of atoms, but still expects a single input and output.
    '''

    pass


class ParallelValidator(ValidatorFilter):

    '''
    This class extends ValidatorFilter and is meant to run checks on multiple Streams at the same
    time (i.e. discrepancy between parallel Streams).
    '''

    # Should probs override _input_check_order(...)

    pass


class MultiBufferValidator(ValidatorFilter):

    pass
