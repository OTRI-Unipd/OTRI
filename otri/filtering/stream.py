from typing import Iterable, overload

class Stream(list):
    '''
    Collection that uses StreamIter as iterator.
    '''
   
    def __iter__(self):
        return StreamIter(self)

    def insert(self, element):
        '''
        Inserts an element at the end of the collection
        '''
        super().append(element)
    
    def is_finished(self):
        try:
            return self.__is_finished
        except AttributeError:
            self.__is_finished = False
            return False

    def close(self):
        self.__is_finished = True

class StreamIter:
    '''
    Iterator that removes the items when using them.
    '''

    def __init__(self, iterable : Iterable):
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