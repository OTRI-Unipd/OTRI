'''
Module containing various `Filter` subclasses, dedicated to finding and marking errors in one or
more `Stream` objects.

Currently the module contains the following abstract classes:

- `ValidatorFilter`: The most basic class, contains logic to check the data and pass it to the
  `_on_ok` and `_on_error` methods, but those are not implemented.
- `LinearValidator`: Pops the atoms one at a time sequentially from its input `Streams`, checks them
  and pushes them to the output `Stream` with the same index. Fully working when overriding `_check`.
- `MonoValidator`: A LinearValidator with a single input and output.
- `BufferedValidator`: `MonoValidator` subclass, can hold back atoms instead of releasing them
  immediately.
- `ParallelValidator`: subclass of both `ParallelFilter` and `ValidatorFilter`, pops atoms in
  batches and checks them together. See the parent class for details on the meaning of "together".
'''

__author__ = "Riccardo De Zen <riccardodezen98@gmail.com>"
__version__ = "1.0"

from ..utils import logger as log
from .exceptions import DEFAULT_KEY, AtomException
from ..filtering.filter import Filter, ParallelFilter

from typing import Mapping, Sequence, Callable, Union, List


class ValidatorFilter(Filter):

    '''
    This Filter is used to apply a check to a list of atoms.
    This is an abstract class and should be further extended implementing at least, the `_check`,
    `_on_ok` and `_on_error` methods.

    The `_check` method should raise some subclass of `AtomException` in case of some kind of
    problem with the atom's data. If the method does not raise any exception, the atom is assumed
    to be ok, and passed on. Only subclasses of `AtomException` are caught.

    This mechanism enforces checking as atomically as possible to better isolate specific errors,
    but nothing prevents from appending various errors directly inside `_check` via the `_add_label`
    method and then considering the atom as "ok".
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
        except AtomException as exc:
            log.v(msg="Data: {}\nException: {}.".format(data, exc))
            self._on_error(data, exc, index)

    def _check(self, data: Mapping):
        '''
        Check a single atom. Must be implemented. Should raise an `AtomException` subclass if the
        atom has one or more problems.

        Parameters:
            data : Mapping
                The data to check.
        Raises:
            Will raise an Exception if there is some problem in the atom.
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
    This class extends `ValidatorFilter` and is meant to be used when running checks on single atoms
    coming from Streams. It simply checks the atoms one at a time and then outputs them on the
    output stream with the same index as the input.

    Must have the same number of inputs and outputs.

    This class can be fully implemented by overriding `_check()` alone.
    '''

    def __init__(self, inputs: Sequence[str], outputs: Sequence[str], check: Callable = None):
        '''
        This Validator expects the same number of Stream inputs and outputs.

        Parameters:
            inputs : str
                Names of the inputs.\n
            outputs : str
                Names of the outputs.\n
            check : Callable
                If you don't want to override the class, you can pass a `Callable` here.
                The `Callable` should expect a single atom as a parameter.
        '''
        # Check same amount of inputs and outputs.
        if len(inputs) != len(outputs):
            raise ValueError("The number of input and output Streams must be the same.")

        super().__init__(inputs, outputs, len(inputs), len(outputs))

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
        and pushes it to the output.

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
        super().__init__([inputs], [outputs], check)


class BufferedValidator(MonoValidator):

    '''
    This class extends `MonoValidator` for use cases where you sometimes need to hold the atoms.
    When you find a suspicious value, call `_hold` to begin pushing the incoming atoms in a buffer
    instead of to the output.

    Call `_release` to push all of the buffer to the output and stop holding back incoming atoms.
    You can call `_buffer_top` to view the next item in the buffer and `_buffer_pop` to release it.

    Errors and warnings will still be appended normally even while holding atoms back.
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
        '''
        # Single input and output super constructor call.
        super().__init__(inputs, outputs, check)
        # Init hold buffer
        self._hold_buffer = list()
        self._holding = False

    def _buffer_top(self) -> Union[Mapping, None]:
        '''
        Returns:
            The first item in the internal buffer. Returns None if the buffer is empty.
        '''
        return self._hold_buffer[0] if self._hold_buffer else None

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

    def _error_all(self, exception: Exception):
        '''
        Append an error to all the atoms in the internal buffer.

        Parameters:
            exception : Exception
                The exception to append.
        '''
        for atom in self._hold_buffer:
            self._add_label(atom, exception)


class ParallelValidator(ValidatorFilter, ParallelFilter):

    '''
    This filter handles finding errors between two Streams, these Streams are read in parallel,
    popping one atom from each of them and consulting them together.

    Due to the nature of this Validator, checking multiple atoms at a time but not keeping them, you
    do not need to raise an exception and can just append them manually when needed, although if an
    exception IS raised, all atoms are considered affected by it and get labeled.
    '''

    def __init__(self, inputs: Sequence[str], outputs: Sequence[str], check: Callable = None):
        '''
        This Validator expects the same number of Stream inputs and outputs.

        Parameters:
            inputs : str
                Names of the inputs.\n
            outputs : str
                Names of the outputs.\n
            check : Callable
                If you don't want to override the class, you can pass a Callable here.
                The Callable should require the atom batch as a parameter.
        '''
        ParallelFilter.__init__(self, inputs, outputs)

        if check is not None:
            self._check = check

    def _on_ok(self, data: List[Mapping], indexes: List[int]):
        '''
        Called if data resulted ok.
        Pushes the atom to the output on the same index it came from.

        Parameters:
            data : List[Mapping]
                The checked data.\n
            index : List[int]
                The indexes from where each atom came from.
        '''
        for atom, index in zip(data, indexes):
            self._push_data(atom, index)

    def _on_error(self, data: List[Mapping], exception: Exception, indexes: List[int]):
        '''
        Called if an error is raised during `_check`. Appends such error to all the atoms in the
        `data` list and pushes them all.

        Parameters:
            data : List[Mapping]
                The checked data.\n
            exception : Exception
                The raised error.\n
            indexes : List[int]
                The index of the input the data has been popped from.
        '''
        for atom, index in zip(data, indexes):
            self._add_label(atom, exception)
            self._push_data(atom, index)

    def _check(self, data: List[Mapping], indexes: List[int]):
        '''
        Parameters:
            data : List[Mapping]
                The atoms retrieved from the inputs.

            indexes : List[int]
                The indexes from which the atoms come from.

        Raises:
            NotImplementedError. This is an abstract class.
        '''
        return ValidatorFilter._check(self, data)

    def _on_data(self, data: List[Mapping], index: List[int]):
        '''
        Called when input data is found.

        Parameters:
            data : List[Mapping]
                The list of atoms from the inputs, one for each of the inputs that are still open.

            indexes : List[int]
                The indexes of the Streams from which the atoms come from.
        '''
        try:
            self._check(data, index)
            self._on_ok(data, index)
        except AtomException as exc:
            log.v(msg="Data: {}\nException: {}.".format(data, exc))
            self._on_error(data, exc, index)


class MultibufferValidator(ParallelValidator):

    '''
    Class implementing a Filter with multiple buffers, reading from multiple Streams in parallel.
    '''
