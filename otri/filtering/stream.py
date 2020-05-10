from typing import Iterable


class Stream(list):
    '''
    Collection that uses StreamIter as iterator.
    '''

    def __iter__(self):
        return StreamIter(self)

    def is_closed(self):
        try:
            return self.__is_closed
        except AttributeError:
            self.__is_closed = False
            return False

    def append(self, element):
        '''
        Raises:
            RuntimeError if the stream is flagged as closed.
        '''
        if not self.is_closed():
            return super().append(element)
        else:
            raise RuntimeError("stream is flagged as closed but it's still being modified")

    def insert(self, index : int, element):
        '''
        Raises:
            RuntimeError if the stream is flagged as closed.
        '''
        if not self.is_closed():
            return super().insert(index, element)
        else:
            raise RuntimeError("stream is flagged as closed but it's still being modified")

    def close(self):
        if not self.is_closed():
            self.__is_closed = True
        else:
            raise RuntimeError("cannot flag stream as closed twice")


class StreamIter:
    '''
    Iterator that removes the items when using them.
    '''

    def __init__(self, iterable: Iterable):
        self.iterable = iterable

    def __next__(self):
        '''
        Pops the first element of the given collection.
        '''
        try:
            value = self.iterable[0]
            del self.iterable[0]
            return value
        except IndexError:
            raise ValueError("empty stream")

    def has_next(self):
        '''
        Calculates if there is another elements by looking at collection's size.
        '''
        return len(self.iterable) > 0
