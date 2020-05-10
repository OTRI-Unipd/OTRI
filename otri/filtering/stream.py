from typing import Iterable


class Stream(list):
    '''
    Collection that uses StreamIter as iterator.
    '''

    def __iter__(self):
        return StreamIter(self)

    def is_finished(self):
        try:
            return self.__is_finished
        except AttributeError:
            self.__is_finished = False
            return False

    def append(self, element):
        '''
        Raises:
            RuntimeError if the stream is flagged as closed.
        '''
        if not self.is_finished():
            return super().append(element)
        else:
            raise RuntimeError("Stream is flagged as closed but it's still being modified")

    def insert(self, index : int, element):
        '''
        Raises:
            RuntimeError if the stream is flagged as closed.
        '''
        if not self.is_finished():
            return super().insert(index, element)
        else:
            raise RuntimeError("Stream is flagged as closed but it's still being modified")

    def close(self):
        self.__is_finished = True


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
        value = self.iterable[0]
        del self.iterable[0]
        return value

    def has_next(self):
        '''
        Calculates if there is another elements by looking at collection's size.
        '''
        return len(self.iterable) > 0
