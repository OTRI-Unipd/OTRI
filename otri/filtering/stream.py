
from collections import deque
from typing import Iterable, Any
from abc import ABC, abstractmethod


__version__ = "2.0"
__author__ = "Riccardo De Zen <riccardodezen98@gmail.com>, Luca Crema <lc.crema@hotmail.com>"


class ClosedStreamError(ValueError):
    pass


class Stream:
    '''
    Interface for a collection of data that can only be popped/dequeued and not topped/peeked.

    Data can be read from it if it's a ReadableStream or pushed into it if it's a WritableStream.

    The stream is defined as open if more data can be added or read (but maybe it's not in the stream at the moment).
    When it's closed the data can be read until the stream is empty but nothing can be added to it.
    '''

    def __init__(self):
        self._closed = False

    def is_closed(self) -> bool:
        '''
        Returns:
            False if new data could be added to the stream, True otherwise.
        '''
        return self._closed

    def close(self):
        '''
        Prevents the stream from getting new data, data contained can still be read.\n

        Raises:\n
            ClosedStreamError - if the stream has already been closed.\n
        '''
        if self.is_closed():
            raise ClosedStreamError("{} is already closed, can not flag it closed again".format(self.__class__))
        self._closed = True


class ReadableStream(ABC, Stream):
    '''
    Stream where data can be read.
    '''

    @abstractmethod
    def has_next(self) -> bool:
        '''
        Checks if the stream contains data ready to be read. Independent from the stream closeness.

        Returns:
            True if the stream contains data, False otherwise.\n
        '''
        raise NotImplementedError()

    @abstractmethod
    def pop(self) -> Any:
        '''
        Returns:
            The first element of the stream.\n
        Raises:
            IndexError - if there is no data available, independently from the stream closeness.
        '''
        raise NotImplementedError()

    # Pop aliases
    read = pop
    dequeue = pop


class WritableStream(ABC, Stream):
    '''
    Stream where data can be pushed.

    Uses Template design pattern to have close-ness checks in common.
    '''

    def push(self, element: Any):
        '''
        Pushes an element into the stream.\n

        Parameters:\n
            element : Any
                A single element to push into the stream.\n
        Raises:\n
            ClosedStreamError - if the stream is flagged as closed.\n
        '''
        if self.is_closed():
            raise ClosedStreamError("{} is flagged as closed but it is still being modified".format(self.__class__))
        self._push(element=element)

    @abstractmethod
    def _push(self, element: Any):
        '''
        Called after common checks.

        Parameters:\n
            element : Any
                A single element to push into the stream.\n
        '''
        pass

    # Push aliases
    write = push
    enqueue = push
    append = push

    def push_all(self, elements: Iterable):
        '''
        Pushes multiple elements inside the stream.\n

        Parameters:\n
            elements : Iterable
                A collection of elements to push into the stream.\n
        Raises:\n
            ClosedStreamError - if the stream is flagged as closed.\n
        '''
        if self.is_closed():
            raise ClosedStreamError("{} is flagged as closed but it is still being modified".format(self.__class__))
        self._push_all(elements=elements)

    @abstractmethod
    def _push_all(self, elements: Iterable):
        '''
        Called after common checks.

        Parameters:\n
            elements : Iterable
                A collection of elements to push into the stream.\n
        '''
        pass

    # Push_all aliases
    write_all = push_all
    enqueue_all = push_all
    extend = push_all


class LocalStream(ReadableStream, WritableStream):
    '''
    FIFO queue-like stream.
    '''

    def __init__(self, elements: Iterable = None, closed: bool = False):
        '''
        Parameters:\n
            elements : Iterable
                Any iterable of data to initialize the stream.\n
        closed : bool
                Define if new data can be written in the stream.\n
        '''
        if(elements is not None):
            self._deque = deque(elements)
        else:
            self._deque = deque()
        self._closed = closed

    def __eq__(self, other):
        '''
        Checks for equality. Does not touch the two streams' data.
        '''
        if not isinstance(other, self.__class__):
            return NotImplemented
        return self._deque == other._deque  # and (self.is_closed == other.is_closed)

    def _push(self, element: Any):
        return self._deque.append(element)

    def _push_all(self, elements: Iterable):
        return self._deque.extend(elements)

    def has_next(self) -> bool:
        return len(self._deque) > 0

    def pop(self) -> Any:
        return self._deque.popleft()

    # Pop aliases
    read = pop
    dequeue = pop

    def clear(self) -> list:
        '''
        Removes all elements from the Stream and returns a list containing data.
        '''
        ret_list = list(self._deque)
        self._deque.clear()
        return ret_list


class VoidStream(WritableStream):
    '''
    A Stream that discards everything it's added to it.
    Used in filter nets to discard unused data.
    '''

    def _push(self, element: Any):
        '''
        Discards passed data.
        '''
        del element

    def _push_all(self, elements: Iterable):
        '''
        Discards passed data.
        '''
        del elements
