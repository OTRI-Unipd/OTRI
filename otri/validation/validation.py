from typing import Sequence, Callable, Mapping, List, Any, Set, Tuple, Final, Optional
from ..filtering.filter import Filter
from ..filtering.stream import Stream
from .exceptions import *

ATOM_OK: Final = 0
ATOM_HOLD: Final = 1


class ValidatorFilter(Filter):

    '''
    This Filter is used to apply a check to a list of atoms.
    This is an abstract class and should be further extended implementing the `_check(atom)` method.

    The `_check(atom)` method should return either `ATOM_OK` or `ATOM_HOLD`, or throw an `AtomError`
    or `AtomWarning` in case of some kind of problem with the atom's data.

    By default the constants mean the following:
    - OK: The atom has no errors, it should not be modified.
    - UNCLEAR: The atom may or may not have errors, depending on the values of its neighbors. This
    error code should be returned only on ValidatorFilters that keep an atom buffer. No message is
    needed.
    - ERROR and WARNING: The check found an error or a dangerous value in the atom, some kind of
    label should be added to the atom. An example: append a string message to a list on the keys
    `ERR_KEY` `WARN_KEY`.
    '''

    def __init__(self, inputs, outputs, input_count=0, output_count=0):
        '''
        Just an override of the super's constructor. Caches the two basic methods to apply on the
        atom based on the result of `_check(data)`, those being `_on_ok(data)` and `_on_hold(data)`.
        '''
        super().__init__(inputs, outputs, input_count=input_count, output_count=output_count)
        # Methods based on analysis result.
        self._actions = {
            ATOM_OK: self._on_ok,
            ATOM_HOLD: self._on_hold
        }

    def _on_data(self, data: Mapping, index: int):
        '''
        Called when input data is found.

        Parameters:
            data : Mapping
                Popped data from an input.\n
            index : int
                The index of the input the data has been popped from.
        Returns:
            The result of the check on the data.
        '''
        try:
            result = self._check(data)
            self._actions[result](self, data, index)
        except Exception as exc:
            self._on_error(data, exc, index)

    # ? MANDATORY OVERRIDE ---

    def _check(self, data: Mapping) -> Any:
        '''
        Check a single atom. Must be implemented. Will return the result of the analysis on an atom
        or raise an Error if the atom has one or more problems.

        Parameters:
            data : Mapping
                The data to check.
        Returns:
            The result for the analysis on the atom. Although it is meant to be used with `ATOM_OK`
            and `ATOM_HOLD`, any alteration of these return values is ok as long as the filter
            remains constistent.
        '''
        raise NotImplementedError("ValidatorFilter is an abstract class, please extend it.")

    # ? OPTIONAL OVERRIDE ---
    # These are optional because a class may or may not want to implement some of them.

    def _on_ok(self, data: Mapping, index: int):
        '''
        Called if data resulted ok.

        Parameters:
            data : Mapping
                The checked data.\n
            index : int
                The index of the input the data has been popped from.
        '''
        raise NotImplementedError("ValidatorFilter is an abstract class, please extend it.")

    def _on_hold(self, data: Mapping, index: int):
        '''
        Called if the state of the data is unclear, and the analysis needs to be halted.

        Parameters:
            data : Mapping
                The checked data.\n
            index : int
                The index of the input the data has been popped from.
        '''
        raise NotImplementedError("ValidatorFilter is an abstract class, please extend it.")

    def _on_error(self, data: Mapping, exception: Exception, index: int):
        '''
        Called if an error is thrown during the analysis.

        Parameters:
            data : Mapping
                The checked data.\n
            exception : Exception
                The exception that got raised.\n
            index : int
                The index of the input the data has been popped from.
        '''
        raise NotImplementedError("ValidatorFilter is an abstract class, please extend it.")

    # ? CLASS METHODS ---

    @classmethod
    def _add_label(cls, atom: Mapping, exception: Exception):
        '''
        Helper method to add an error or warning label to an atom.
        Add an empty `list` in `atom` for `exception.KEY` if `exception.KEY` is not in the atom's
        keys. Append `value` to the `exception.KEY` field in `atom`.

        If the `exception` does not have the `KEY` attribute, `DEFAULT_KEY` is used instead.

        Parameters:
            atom : Mapping
                The atom to label.\n
            exception : Exception
                The exception to append. Will be converted to String.\n
        Raises:
            `AttributeError` if `key` is already in the atom's keys but it does not lead to a List.
        '''
        try:
            key = exception.KEY
        except AttributeError:
            key = DEFAULT_KEY
        if key not in atom.keys():
            atom[key] = list()
        atom[key].append(str(exception))


class LinearValidator(ValidatorFilter):

    '''
    This class extends ValidatorFilter and is meant to be used when running checks on single atoms
    coming from Streams. Just checks the atoms one at a time and then outputs them on the output
    stream with the same index as the input.

    This class does not keep an internal buffer for atoms that need to stay on hold, it assumes the
    analysis will always result in `ATOM_OK` or some kind of error, so `_check(data)`should never
    return `ATOM_HOLD`.

    Refer to the subclass `BufferedValidator` for a buffered alternative.

    This class can be fully implemented by overriding `_check()` alone.
    '''

    def __init__(self, inputs: Sequence[str], outputs: Sequence[str], input_count: int = 0,
                 output_count: int = 0, check: Callable = None):
        '''
        This Validator expects the same number of Stream inputs and outputs.

        Parameters:
            inputs : str
                Names of the inputs.\n
            outputs : str
                Names of the outputs.\n
            input_count : int
                Number of input Streams.\n
            output_count : int
                Number of output Streams.\n
            check : Callable
                If you don't want to override the class, you can pass a Callable here.
                The Callable should require only the atom as a parameter.
        '''
        # Check same amount of inputs and outputs.
        if input_count != output_count:
            raise ValueError("The number of input and output Streams must be the same.")

        super().__init__(inputs, outputs, input_count, output_count)

        # Overwrite check.
        if check:
            self._check = check

    def _on_ok(self, data: Mapping, index: int):
        '''
        Called if data resulted ok.
        Pushes the atom to the output on the same index it came from.

        Parameters:
            data : Mapping
                The checked data.
        '''
        self._push_data(data, index)

    def _on_error(self, data: Mapping, exception: Exception, index: int):
        '''
        Called if an error is thrown during the analysis. Adds an error/warning label to the atom
        and pushes it.

        Parameters:
            data : Mapping
                The checked data.\n
            index : int
                The index of the input the data has been popped from.
        '''
        self._add_label(data, exception)
        self._push_data(data, index)


class MonoValidator(LinearValidator):
    '''
    A `LinearValidator` for a single Stream.
    '''

    def __init__(self, inputs: str, outputs: str, check: Callable = None):
        '''
        Parameters:
            inputs : str
                Name for the single input stream.\n
            outputs : str
                Name for the single output stream.\n
            check : Callable
                If you don't want to override the class, you can pass a Callable here.
                The Callable should require only the atom as a parameter.
        '''
        super().__init__([inputs], [outputs], 1, 1, check)


class BufferedValidator(MonoValidator):

    '''
    This class extends MonoValidator for use cases where you sometimes need to hold the atoms.
    When you find a suspicious value, return `ATOM_HOLD` to begin pushing the incoming atoms in an
    `_hold_buffer` buffer instead of to the output.

    Call `_release()` to push all of the buffer to the output and stop holding back incoming atoms.
    You can just manually pop the items if you need to release them one at a time. 

    Errors and warnings will still be appended normally either way.
    '''

    def __init__(self, inputs: str, outputs: str, check: Callable):
        '''
        Parameters:
            inputs : str
                Name for the single input stream.\n
            outputs : str
                Name for the single output stream.\n
            check : Callable
                If you don't want to override the class, you can pass a Callable here.
        '''
        # Single input and output super constructor call.
        super().__init__(inputs, outputs)
        # Init hold buffer
        self._hold_buffer = list()
        self._holding = False

    def _on_ok(self, data: Mapping, index: int = 0):
        '''
        Called if data resulted ok. Pushes atom to the output or holds it if it should.

        Parameters:
            data : Mapping
                The checked data.\n
            index : int
                The index of the input Stream from which the atom came.
        '''
        if self._holding:
            self._hold_buffer.append(data)
        else:
            self._push_data(data, index)

    def _on_hold(self, data: Mapping, index: int = 0):
        '''
        Called if the state of the data is unclear, and the analysis needs to be postponed.
        Puts the atom in the hold buffer and prepares to hold the following atoms as well.

        Parameters:
            data : Mapping
                The checked data.\n
            index : int
                The index of the input Stream from which the atom came.
        '''
        self._hold_buffer.append(data)
        self._holding = True

    def _on_error(self, data: Mapping, exception: Exception, index: int = 0):
        '''
        Called if an error is thrown during the analysis. Adds an error/warning label to the atom
        and pushes it.

        Parameters:
            data : Mapping
                The checked data.\n
            index : int
                The index of the input the data has been popped from.
        '''
        self._add_label(data, exception)
        if self._holding:
            self._hold_buffer.append(data)
        else:
            self._push_data(data, index)

    def _release(self):
        '''
        Release all of the held atoms and stop holding new ones back.
        '''
        self._holding = False
        while self._hold_buffer:
            # Index is always 0 anyway
            self._push_data(self._hold_buffer.pop())
