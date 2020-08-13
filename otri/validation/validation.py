from typing import Sequence, Callable, Mapping, List, Any, Set, Tuple, Final, Optional
from ..filtering.filter import Filter
from ..filtering.stream import Stream
from .exceptions import *


class ValidatorFilter(Filter):

    '''
    This Filter is used to apply a check to a list of atoms.
    This is an abstract class and should be further extended implementing the `_check(atom)` method.

    The `_check(atom)` method should raise an `AtomError` or `AtomWarning` in case of some kind of
    problem with the atom's data. If the method does not raise any exception, the atom is assumed
    to be ok, and passed on.

    This mechanism enforces checking as atomically as possible to better isolate specific errors.
    '''

    def _on_data(self, data: Mapping, index: int):
        '''
        Called when input data is found.

        Parameters:
            data : Mapping
                Popped data from an input.

            index : int
                The index of the input the data has been popped from.

        Returns:
            The result of the check on the data.
        '''
        try:
            self._check(data)
            self._on_ok(data, index)
        except Exception as exc:
            self._on_error(data, exc, index)

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
                The atom to label.

            exception : Exception
                The exception to append. Will be converted to String.

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
    When you find a suspicious value, call `_hold()` to begin pushing the incoming atoms in a buffer
    instead of to the output.

    Call `_release()` to push all of the buffer to the output and stop holding back incoming atoms.
    You can call `_buffer_top()` to view the next item in the buffer and `_buffer_pop()` to release
    it alone.

    Errors and warnings will still be appended normally even while holding atoms back.
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

    def _buffer_top(self) -> Mapping:
        '''
        Returns:
            The first item in the internal buffer.
        '''
        return self._hold_buffer[0]

    def _buffer_pop(self):
        '''
        Pops the first item from the buffer and pushes it to the output.
        '''
        # Index is always 0 since this class extends `MonoValidator`.
        self._push_data(self._hold_buffer.pop())

    def _hold(self):
        '''
        Called if the state of the data is unclear, and the analysis needs to be postponed.
        Sets a flag indicating the future atoms should all be added to an internal buffer instead
        of being released.
        '''
        self._holding = True

    def _release(self):
        '''
        Release all of the held atoms and stop holding new ones back.
        '''
        self._holding = False
        while self._hold_buffer:
            self._buffer_pop()

    def _on_ok(self, data: Mapping, index: int = 0):
        '''
        Called if data analysis threw no error. Pushes atom to the output or holds it if the
        `_holding` flag is `True`.

        Parameters:
            data : Mapping
                The checked data.

            index : int
                The index of the input Stream from which the atom came.
        '''
        if self._holding:
            self._hold_buffer.append(data)
        else:
            self._push_data(data, index)

    def _on_error(self, data: Mapping, exception: Exception, index: int = 0):
        '''
        Called if an error is thrown during the analysis. Adds an error/warning label to the atom
        and pushes it, or holds it if the `_holding` flag is `True`.

        Parameters:
            data : Mapping
                The checked data.

            index : int
                The index of the input the data has been popped from.
        '''
        self._add_label(data, exception)
        if self._holding:
            self._hold_buffer.append(data)
        else:
            self._push_data(data, index)
