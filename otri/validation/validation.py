'''
Module containing Filters aimed to find and point out errors or other problems.
'''

__author__ = "Riccardo De Zen <riccardodezen98@gmail.com>"
__version__ = "1.0"


from typing import Sequence, Callable, Mapping, List, Any, Set, Tuple, Final, Optional, TypeVar
from ..filtering.filter import Filter, ParallelFilter
from ..filtering.stream import Stream
from ..utils import logger as log
from .exceptions import *


class ValidatorFilter(Filter):

    '''
    This Filter is used to apply a check to a list of atoms.
    This is an abstract class and should be further extended implementing the `_check(atom)` method.

    The `_check(atom)` method should add an `AtomError` or `AtomWarning` in case of some kind of
    problem with the atom's data. If the method does not raise any exception, the atom is assumed
    to be ok, and passed on.

    This mechanism enforces checking as atomically as possible to better isolate specific errors,
    but nothing prevents from appending errors directly inside `_check(atom)` and then considering
    the atom as "ok".
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
    This class extends ValidatorFilter and is meant to be used when running checks on single atoms
    coming from Streams. Just checks the atoms one at a time and then outputs them on the output
    stream with the same index as the input.

    Must have the same number of inputs and outputs.

    Refer to the subclass `BufferedValidator` for a buffered alternative.

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
                If you don't want to override the class, you can pass a Callable here.
                The Callable should require only the atom as a parameter.
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
        super().__init__([inputs], [outputs], check)


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

    def _error_all(self, exception: Exception):
        '''
        Append an error to all the atoms in the internal buffer.

        Parameters:
            exception : Exception
                The exception to append.
        '''
        for atom in self._hold_buffer:
            self._add_label(atom, exception)


class ClusterValidator(BufferedValidator):
    '''
    Verify that no clusters bigger than N are formed for a set of keys.

    The Validator always holds atoms until it has determined that there is or there is not a cluster
    big enough to trigger a warning.
    '''

    def __init__(self, inputs: str, outputs: str, key: str, limit: int):
        '''
        Parameters:
            inputs : str
                Name for the single input stream.\n
            outputs : str
                Name for the single output stream.\n
            key : str
                The key to monitor for clusters.\n
            limit: str
                Any cluster strictly higher than limit will be marked.
        '''
        # Single input and output super constructor call.
        super().__init__(inputs, outputs)

        self._cluster_key = key
        self._cluster_limit = limit
        self._cluster_size = 0

        # Hold by default.
        self._holding = True

    def _check(self, data: Mapping):
        '''
        Check a single atom.
        - If the internal buffer is empty it won't do anything.
        - If the internal buffer is not empty and the value of the top atom in the buffer is equal
        to the current atom's value, increase the cluster size.
        - If the internal buffer is not empty and the value of the top atom in the buffer is different
        than the current atom's value, look at the cluster size, if it's higher than the given limit,
        append a ClusterWarning on all of the buffer's atoms and release them. Reset the cluster size.

        Parameters:
            data : Mapping
                The data to check.
        Raises:
            Will raise an Exception if it found.
        '''
        # Empty buffer, do nothing.
        if not self._hold_buffer:
            self._cluster_size = 1
            return

        if self._buffer_top()[self._cluster_key] == data[self._cluster_key]:
            self._cluster_size += 1
        else:
            self._check_cluster()

    def _check_cluster(self):
        '''
        When a cluster is found:
        - Mark all the atoms in the buffer
        - Release them
        - Reset cluster size
        '''
        if self._cluster_size > self._cluster_limit:
            print("CLUSTER")
            self._error_all(ClusterWarning({self._cluster_key, self._cluster_size}))
        # Either way reset cluster.
        self._release()
        self._holding = True
        self._cluster_size = 1

    def _on_inputs_closed(self):
        '''
        All of the inputs are closed and no more data is available.
        The filter empties the buffer and closes its outputs.
        '''
        self._check_cluster()
        super()._on_inputs_closed()


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
                Must take the two values as parameters and return a boolean.
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
        if last != None:
            first = last[key]
            second = data[key]
            if not self._continuity(first, second):
                # Mark the other atom too.
                error = ContinuityError({key: first, key: second})
                self._add_label(self._last_atom, error)
                self._last_atom = data
                raise error
        # Cache this atom for later, but still release it.
        self._last_atom = data


class ParallelValidator(ValidatorFilter, ParallelFilter):

    '''
    This filter handles finding errors between two Streams, these Streams are read in parallel,
    popping one atom from each of them and consulting them together.

    Due to the nature of this Validator, checking multiple atoms at a time but not keeping them, you
    do not need to raise an exception and can just append them manually when needed.

    If an error is raised, all atoms are considered affected by it and get labeled.
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
        # Check same amount of inputs and outputs.
        if len(inputs) != len(outputs):
            raise ValueError("The number of input and output Streams must be the same.")

        ParallelFilter.__init__(self, inputs, outputs)

        if check:
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
        for i in range(len(data)):
            self._push_data(data[i], indexes[i])

    def _on_error(self, data: List[Mapping], exception: Exception, indexes: List[int]):
        '''
        Called if an error is thrown during the analysis. Adds an error/warning label to the atom
        and pushes it.

        Parameters:
            data : List[Mapping]
                The checked data.\n
            exception : Exception
                The raised error.\n
            indexes : List[int]
                The index of the input the data has been popped from.
        '''
        for i in range(len(data)):
            self._add_label(data[i], exception)
            self._push_data(data[i], indexes[i])

    def _check(self, data: List[Mapping]):
        '''
        Parameters:
            data : List[Mapping]
                The atoms retrieved from the inputs.

        Raises:
            NotImplementedError. This is an abstract class.
        '''
        return super()._check(data)
