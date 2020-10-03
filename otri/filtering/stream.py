
from collections import deque
from typing import Iterable, Any


class ClosedStreamError(ValueError):
    pass


class Stream:
    '''
    FIFO queue-like collection that can be open to new data or closed.
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
            raise ClosedStreamError("stream is flagged as closed but it is still being modified")
        return self._deque.append(element)

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
            raise ClosedStreamError("stream is flagged as closed but it is still being modified")
        return self._deque.extend(elements)

    # Push_all aliases
    write_all = push_all
    enqueue_all = push_all
    extend = push_all

    def has_next(self) -> bool:
        '''
        Returns:
            True if the stream contains data, false otherwise.\n
        '''
        return len(self._deque) > 0

    def pop(self) -> Any:
        '''
        Returns:
            The first element in the First-in First-out order.\n
        Raises:
            IndexError - if there is no data available.
        '''
        return self._deque.popleft()

    # Pop aliases
    read = pop
    dequeue = pop

    def clear(self):
        '''
        Removes all elements from the Stream.
        '''
        self._deque.clear()

    def is_closed(self) -> bool:
        '''
        Returns:
            True if new data cannot be added to the stream, False otherwise.
        '''
        return self._closed

    def close(self):
        '''
        Prevents the stream from getting new data, data contained can still be iterated.\n

        Raises:\n
            ClosedStreamError - if the stream has already been closed.\n
        '''
        if self.is_closed():
            raise ClosedStreamError("stream is already closed, can not flag it closed again")
        self._closed = True


def VoidStream(Stream):
    '''
    A Stream that discards everything it's added to it.
    Used in filter nets to discard unused data.
    '''

    def push(self, element: Any):
        '''
        Discards passed data.\n

        Parameters:\n
            element : Any
                A single element to discard.\n
        Raises:\n
            ClosedStreamError - if the stream is flagged as closed.\n
        '''
        if self.is_closed():
            raise ClosedStreamError("stream is flagged as closed but it is still being modified")
        del element

    def push_all(self, elements: Iterable):
        '''
        Discards passed data.\n

        Parameters:\n
            elements : Iterable
                A collection of elements to discard.\n
        Raises:\n
            ClosedStreamError - if the stream is flagged as closed.\n
        '''
        if self.is_closed():
            raise ClosedStreamError("stream is flagged as closed but it is still being modified")
        del elements

    def pop(self) -> Any:
        '''
        Returns:
            The first element in the First-in First-out order.\n
        Raises:
            IndexError - if there is no data available.
        '''
        raise IndexError("cannot read data from Void Stream")
