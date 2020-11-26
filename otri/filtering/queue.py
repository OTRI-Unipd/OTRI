
from collections import deque
from typing import Iterable, Any
from abc import ABC, abstractmethod


__version__ = "2.0"
__author__ = "Riccardo De Zen <riccardodezen98@gmail.com>, Luca Crema <lc.crema@hotmail.com>"


class ClosedQueueError(ValueError):
    pass


class Queue:
    '''
    Interface for a collection of data that can only be popped/dequeued and not topped/peeked.

    Data can be read from it if it's a ReadableQueue or pushed into it if it's a WritableQueue.

    The queue is defined as open if more data can be added or read (but maybe it's not in the queue at the moment).
    When it's closed the data can be read until the queue is empty but nothing can be added to it.
    '''

    def __init__(self):
        self._closed = False

    def is_closed(self) -> bool:
        '''
        Returns:
            False if new data could be added to the queue, True otherwise.
        '''
        return self._closed

    def close(self):
        '''
        Prevents the queue from getting new data, data contained can still be read.\n

        Raises:\n
            ClosedQueueError - if the queue has already been closed.\n
        '''
        if self.is_closed():
            raise ClosedQueueError("{} is already closed, can not flag it closed again".format(self.__class__))
        self._closed = True


class ReadableQueue(ABC, Queue):
    '''
    Queue where data can be read.
    '''

    @abstractmethod
    def has_next(self) -> bool:
        '''
        Checks if the queue contains data ready to be read. Independent from the queue close state.

        Returns:
            True if the queue contains data, False otherwise.\n
        '''
        raise NotImplementedError()

    def pop(self) -> Any:
        '''
        Removes an element from the queue and returns it.\n
        It's recommended making sure there's some data to read with has_next().\n

        Returns:
            The first element of the queue.\n
        Raises:
            IndexError - if there is no data available, independently from the queue close state.
        '''
        return self._pop()

    @abstractmethod
    def _pop(self) -> Any:
        '''
        Called after common checks. Removes one element of the queue.\n
        '''
        pass

    # Pop aliases
    read = pop
    dequeue = pop


class WritableQueue(ABC, Queue):
    '''
    Queue where data can be pushed.

    Uses Template design pattern to have close-ness checks in common.
    '''

    def push(self, element: Any):
        '''
        Pushes an element into the queue.\n

        Parameters:\n
            element : Any
                A single element to push into the queue.\n
        Raises:\n
            ClosedQueueError - if the queue is flagged as closed.\n
        '''
        if self.is_closed():
            raise ClosedQueueError("{} is flagged as closed but it is still being modified".format(self.__class__))
        self._push(element=element)

    @abstractmethod
    def _push(self, element: Any):
        '''
        Called after common checks.

        Parameters:\n
            element : Any
                A single element to push into the queue.\n
        '''
        pass

    # Push aliases
    write = push
    enqueue = push
    append = push

    def push_all(self, elements: Iterable):
        '''
        Pushes multiple elements inside the queue.\n

        Parameters:\n
            elements : Iterable
                A collection of elements to push into the queue.\n
        Raises:\n
            ClosedQueueError - if the queue is flagged as closed.\n
        '''
        if self.is_closed():
            raise ClosedQueueError("{} is flagged as closed but it is still being modified".format(self.__class__))
        self._push_all(elements=elements)

    @abstractmethod
    def _push_all(self, elements: Iterable):
        '''
        Called after common checks.

        Parameters:\n
            elements : Iterable
                A collection of elements to push into the queue.\n
        '''
        pass

    # Push_all aliases
    write_all = push_all
    enqueue_all = push_all
    extend = push_all


class LocalQueue(ReadableQueue, WritableQueue):
    '''
    FIFO queue-like queue.
    '''

    def __init__(self, elements: Iterable = None, closed: bool = False):
        '''
        Parameters:\n
            elements : Iterable
                Any iterable of data to initialize the queue.\n
        closed : bool
                Define if new data can be written in the queue.\n
        '''
        if(elements is not None):
            self._deque = deque(elements)
        else:
            self._deque = deque()
        self._closed = closed

    def __eq__(self, other):
        '''
        Checks for equality. Does not touch the two queues' data.
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

    def _pop(self) -> Any:
        return self._deque.popleft()

    def clear(self) -> list:
        '''
        Removes all elements from the Queue and returns a list containing data.
        '''
        ret_list = list(self._deque)
        self._deque.clear()
        return ret_list

    to_list = clear


class VoidQueue(WritableQueue):
    '''
    A Queue that discards everything it's added to it.
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
