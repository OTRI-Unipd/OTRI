from typing import Iterable


class Stream(list):
    '''
    Collection that uses StreamIter as iterator.
    '''

    def __init__(self, iterable: Iterable = None, is_closed: bool = False):
        '''
        Parameters:
            iterable : Iterable
                An iterable of any kind.
            is_closed : bool
                Define if new data can be added to the stream.
        '''
        if(iterable != None):
            list.__init__(self, iterable)
        else:
            list.__init__(self)
        self.__is_closed = is_closed

    def __iter__(self):
        return StreamIter(self)

    def is_closed(self) -> bool:
        '''
        Defines if new data can be added to the stream.
        '''
        return self.__is_closed

    def append(self, element):
        '''
        Raises:
            RuntimeError if the stream is flagged as closed.
        '''
        if not self.is_closed():
            return super(Stream, self).append(element)
        else:
            raise RuntimeError(
                "stream is flagged as closed but it's still being modified")

    def insert(self, index: int, element):
        '''
        Raises:
            RuntimeError if the stream is flagged as closed.
        '''
        if not self.is_closed():
            return super(Stream, self).insert(index, element)
        else:
            raise RuntimeError(
                "stream is flagged as closed but it's still being modified")

    def close(self):
        '''
        Prevents the stream from getting new data, data contained can still be iterated.

        Raises:
            RuntimeError if the stream has already been closed.
        '''
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
            raise StopIteration("empty stream")

    def has_next(self):
        '''
        Calculates if there is another elements by looking at collection's size.
        '''
        return len(self.iterable) > 0
