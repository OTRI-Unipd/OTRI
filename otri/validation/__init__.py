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
from .exceptions import DEFAULT_KEY
from ..filtering.filter import Filter, ParallelFilter

from typing import Mapping, Sequence, Callable, Union, List, Any


def append_label(data: Mapping, label: Any):
    '''
    Helper method to add an error or warning label to an atom.
    Add an empty `list` in `atom` for `label.KEY` if `label.KEY` is not in the atom's
    keys. Append `label` to the `label.KEY` field in `atom`.

    If `label` does not have the `KEY` attribute, `DEFAULT_KEY` is used instead.

    Parameters:
        data : Mapping
            The atom to label.

        label : Any
            The label to append. Will be converted to String via `__repr__`.

    Raises:
        `AttributeError` if `key` is already in the atom's keys but it does not lead to a List.
    '''
    try:
        key = label.KEY
    except AttributeError:
        key = DEFAULT_KEY
    # Not using `data.setdefault(key, list())` to avoid creating lists every time.
    if key not in data.keys():
        data[key] = list()
    data[key].append(repr(label))


class ValidatorFilter(Filter):

    '''
    This Filter is used to apply a check to a list of atoms.
    This is an abstract class and should be further extended implementing at least, the `_check`,
    `_on_ok` and `_on_error` methods.

    The `_check` method should return some subclass of `AtomException` in case of some kind of
    problem with the atom's data. If the method does not return any value, the atom is assumed
    to be ok, and passed on. Any value can be returned.
    '''

    def _on_data(self, data: Mapping, index: int):
        '''
        Called when input data is found.

        Parameters:
            data : Mapping
                Popped data from an input.

            index : int
                The index of the input the data has been popped from.
        '''
        result = self._check(data)
        if result is None:
            self._on_ok(data, index)
        else:
            log.v(msg="{}. Data: {}\nAnalysis: {}.".format(self, data, result))
            self._on_result(data, result, index)

    def _check(self, data: Mapping) -> Union[Any, None]:
        '''
        Check a single atom. Must be implemented. Should return None if the atom is ok or some value
        indicating the problem. See `AtomException` for examples.

        Parameters:
            data : Mapping
                The data to check.
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

    def _on_result(self, data: Mapping, result: Any, index: int):
        '''
        Called if something was returned by `_check`.

        Parameters:
            data : Mapping
                The checked data.

            result : Any
                The result of the analysis.

            index : int
                The index of the input the data has been popped from.
        '''
        raise NotImplementedError("ValidatorFilter is an abstract class, please extend it.")

    # This separation of the method from the class allows to override it globally, on class level or
    # on object level, as needed.

    @classmethod
    def _add_label(cls, data: Mapping, label: Any):
        '''
        How the `ValidatorFilter` handles appending a label to an atom. This uses directly the
        `append_label` method of this module.
        '''
        append_label(data, label)


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
                Names of the inputs.

            outputs : str
                Names of the outputs.

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

    def _on_result(self, data: Mapping, result: Any, index: int):
        '''
        Called if `_check` returned something. Labels the atom and pushes it to the output.

        Parameters:
            data : Mapping
                The checked data.

            result : Any
                The result of `_check`.

            index : int
                The index of the input the data has been popped from.
        '''
        self._add_label(data, result)
        self._push_data(data, index)


class MonoValidator(LinearValidator):
    '''
    A `LinearValidator` for a single Stream.
    '''

    def __init__(self, inputs: str, outputs: str, check: Callable = None):
        '''
        Parameters:
            inputs : str
                Name for the single input stream.

            outputs : str
                Name for the single output stream.

            check : Callable
                If you don't want to override the class, you can pass a Callable here.
                The Callable should require only the atom as a parameter.
        '''
        super().__init__([inputs], [outputs], check)


class BufferedValidator(LinearValidator):

    '''
    This class extends `LinearValidator` for use cases where you need to compare the atoms with
    their neighbors.
    Call `_hold` to begin pushing the incoming atoms in a buffer instead of to the output.
    The buffers are one for each input Stream.

    Call `_release` to push all of the buffers' contents to the output and stop holding back
    incoming atoms.
    You can call `_buffer_top` to view the next item in the buffer and `_buffer_pop` to release it.

    Return values from `_check` are used normally as labels.

    If the input Streams are closed and empty, the buffers will all be released.
    '''

    def __init__(self, inputs: Sequence[str], outputs: Sequence[str], check: Callable = None):
        '''
        Parameters:
            inputs : Sequence[str]
                Name for the input streams.

            outputs : Sequence[str]
                Name for the output streams.

            check : Callable
                If you don't want to override the class, you can pass a Callable here.
        '''
        # Single input and output super constructor call.
        super().__init__(inputs, outputs, check)
        # Init hold buffer
        self._hold_buffer = [list() for _ in range(len(inputs))]
        self._holding = [False] * len(inputs)

    def _buffer_top(self, index: int = 0) -> Union[Mapping, None]:
        '''
        Returns:
            The first item in the internal buffer for the given inde.
            Returns None if the buffer is empty.
        '''
        return self._hold_buffer[index][0] if self._hold_buffer[index] else None

    def _buffer_pop(self, index: int = 0):
        '''
        Pops the first item from the buffer at the given index and pushes it to the output.
        '''
        atom = self._hold_buffer[index][0]
        del self._hold_buffer[index][0]
        self._push_data(atom, index)

    def _hold(self, index: int = 0):
        '''
        Sets a flag indicating the future atoms should all be added to an internal buffer instead
        of being released.
        '''
        self._holding[index] = True

    def _release(self, index: int = 0):
        '''
        Release all of the held atoms and stop holding new ones back.
        '''
        self._holding[index] = False
        buffer = self._hold_buffer[index]
        while buffer:
            self._buffer_pop(index)

    def _release_all(self):
        '''
        Same as `_release` but on all buffers.
        '''
        for i in range(len(self._holding)):
            self._release(i)

    def _on_ok(self, data: Mapping, index: int = 0):
        '''
        Called if `_check` returned `None`. Pushes atom to the output, or to the internal buffer
        if the `_holding` flag is `True`.

        Parameters:
            data : Mapping
                The checked data.

            index : int
                The index of the input Stream from which the atom came.
        '''
        if self._holding[index]:
            self._hold_buffer[index].append(data)
        else:
            self._push_data(data, index)

    def _on_result(self, data: Mapping, result: Any, index: int = 0):
        '''
        Called if `_check` returned something. Labels the atom and pushes it, same as `_on_ok`.

        Parameters:
            data : Mapping
                The checked data.

            result : Any
                The result of `_check`.

            index : int
                The index of the input the data has been popped from.
        '''
        self._add_label(data, result)
        if self._holding[index]:
            self._hold_buffer[index].append(data)
        else:
            self._push_data(data, index)

    def _label_all(self, label: Any):
        '''
        Label all the atoms in the internal buffer.

        Parameters:
            label : Any
                The label to append.
        '''
        for buffer in self._hold_buffer:
            for atom in buffer:
                self._add_label(atom, label)

    def _on_inputs_closed(self):
        '''
        When the inputs are empty and closed everything gets released before closing the outputs.
        '''
        self._release_all()
        return super()._on_inputs_closed()


class ParallelValidator(ValidatorFilter, ParallelFilter):

    '''
    This filter handles finding errors between multiple Streams, in parallel,
    popping one atom from each of them and consulting them together.

    If a value is returned by _check, all parallel atoms are considered affected by it and get
    labeled.
    '''

    def __init__(self, inputs: Sequence[str], outputs: Sequence[str], check: Callable = None):
        '''
        This Validator expects the same number of Stream inputs and outputs.

        Parameters:
            inputs : str
                Names of the inputs.

            outputs : str
                Names of the outputs.

            check : Callable
                If you don't want to override the class, you can pass a Callable here.
                The Callable should require the atom batch as a parameter.
        '''
        ParallelFilter.__init__(self, inputs, outputs)

        if check is not None:
            self._check = check

    def _on_ok(self, data: List[Mapping], indexes: List[int]):
        '''
        Called if `_check` returned `None`.
        Pushes the atom to the output on the same index it came from.

        Parameters:
            data : List[Mapping]
                The checked data.

            index : List[int]
                The indexes from where each atom came from.
        '''
        for atom, index in zip(data, indexes):
            self._push_data(atom, index)

    def _on_result(self, data: List[Mapping], result: Any, indexes: List[int]):
        '''
        Called if `_check` returned something.
        Labels all the atoms in the `data` list and pushes them all.

        Parameters:
            data : List[Mapping]
                The checked data.

            result : Any
                The result of `_check`.

            indexes : List[int]
                The index of the input the data has been popped from.
        '''
        for atom, index in zip(data, indexes):
            append_label(atom, result)
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

    def _on_data(self, data: List[Mapping], indexes: List[int]):
        '''
        Called when input data is found.

        Parameters:
            data : List[Mapping]
                The list of atoms from the inputs, one for each of the inputs that are still open.

            indexes : List[int]
                The indexes of the Streams from which the atoms come from.
        '''
        result = self._check(data, indexes)
        if result is None:
            self._on_ok(data, indexes)
        else:
            log.v(msg="{}. Data: {}\nAnalysis: {}.".format(self, data, result))
            self._on_result(data, result, indexes)


class ParallelBufferValidator(ParallelValidator):

    '''
    Class implementing a Filter with multiple buffers, reading from multiple Streams in parallel.
    This is very similar to `BufferedValidator`, except the data is always passed as a list of
    values coming from input Streams only when available.
    There is only one internal buffer. `_buffer_top`, `_buffer_pop` and `_release` behave
    accordingly.
    '''

    def __init__(self, inputs: Sequence[str], outputs: Sequence[str], check: Callable = None):
        '''
        This Validator expects the same number of Stream inputs and outputs.

        Parameters:
            inputs : str
                Names of the inputs.

            outputs : str
                Names of the outputs.

            check : Callable
                If you don't want to override the class, you can pass a Callable here.
                The Callable should require the atom batch as a parameter.
        '''
        ParallelFilter.__init__(self, inputs, outputs)

        # Buffer for the atom batches
        self._hold_buffer = list()
        # Buffer for the atom indexes
        self._index_buffer = list()
        # Hold flag
        self._holding = False

        if check is not None:
            self._check = check

    def _on_ok(self, data: List[Mapping], indexes: List[int]):
        '''
        Called if data resulted ok.
        Pushes the atoms to the output on the same index they came from.

        Parameters:
            data : List[Mapping]
                The checked data.

            indexes : List[int]
                The indexes from where each atom came from.
        '''
        if self._holding:
            self._hold_buffer.append(data)
            self._index_buffer.append(indexes)
        else:
            for atom, index in zip(data, indexes):
                self._push_data(atom, index)

    def _on_result(self, data: List[Mapping], result: Any, indexes: List[int]):
        '''
        Called if an error is raised during `_check`. Appends such error to all the atoms in the
        `data` list and pushes them all.

        Parameters:
            data : List[Mapping]
                The checked data.

            result : Any
                The result of `_check`.

            indexes : List[int]
                The index of the input the data has been popped from.
        '''
        if self._holding:
            for atom in data:
                self._add_label(atom, result)
            self._hold_buffer.append(data)
            self._index_buffer.append(indexes)
        else:
            for atom, index in zip(data, indexes):
                self._add_label(atom, result)
                self._push_data(atom, index)

    def _buffer_top(self) -> Union[List[Mapping], None]:
        '''
        Returns:
            The first item in the internal buffer for the given inde.
            Returns None if the buffer is empty.
        '''
        return self._hold_buffer[0] if self._hold_buffer else None

    def _buffer_pop(self):
        '''
        Pops the first item group from the buffer at the given index and pushes it to the output.
        '''
        data = self._hold_buffer[0]
        del self._hold_buffer[0]
        indexes = self._index_buffer[0]
        del self._index_buffer[0]
        for atom, index in zip(data, indexes):
            self._push_data(atom, index)

    def _hold(self):
        '''
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

    def _on_inputs_closed(self):
        '''
        When the inputs are empty and closed everything gets released before closing the outputs.
        '''
        self._release()
        return super()._on_inputs_closed()
