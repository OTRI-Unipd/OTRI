from typing import Callable, Iterable
from .filter_layer import FilterLayer


class FilterList:
    '''
    Ordered collection of filter layers.
    '''

    def __init__(self, layers: Iterable[FilterLayer] = None):
        if layers == None:
            self.layers = []
        else:
            self.layers = layers

    def add_layer(self, layer: FilterLayer):
        '''
        Appends a layer at the end of the list

        Parameters:
            layer : FilterLayer
                Non-empty layer of filters.
        '''
        self.layers.append(layer)

    def execute(self, on_atom_output: Callable = None, on_execute_finished: Callable = None):
        '''
        Starts working on the origin stream with the given filter layers

        Parameter:
            source_streams : Collection[Streams]
                Atoms collections. Required iterator that has `has_next()` method.
        '''
        # Grab the output streams
        last_output_streams = self.layers[len(
            self.layers) - 1][0].get_output_streams()
        last_output_iterators = [x.__iter__() for x in last_output_streams]

        self.__execute_0(last_output_iterators, on_atom_output)

        if (on_execute_finished != None):
            on_execute_finished()

    def __is_all_finished(self) -> bool:
        '''
        Checks if the last filter layer's filters are flagged as finished
        '''
        if not self.layers[len(self.layers) - 1].is_finished():
            return False
        return True

    def __execute_0(self, last_output_iterators, on_atom_output):
        while(not self.__is_all_finished()):
            for filter_layer in self.layers:
                for fil in filter_layer:
                    fil.execute()
            # If any filter has outputted any atom, call the on_atom_output method
            if on_atom_output != None:
                for iterator in last_output_iterators:
                    while(iterator.has_next()):
                        on_atom_output(next(iterator))

    def __execute_1(self, last_output_iterators, on_atom_output):
        index = 0
        while not self.layers[len(self.layers)-1].is_finished():
            # print("i:",index)
            for fil in self.layers[index]:
                for output_stream in fil.get_output_streams():
                    if(output_stream.__iter__().has_next()):
                        index += 1
                        break
                else:
                    # if it terminates normally
                    #print("layer {} has no output".format(index))
                    for input_stream in fil.get_input_streams():
                        if(input_stream.__iter__().has_next()):
                            break
                    else:
                        #print("layer {} has no input either".format(index))
                        index -= 1
                        continue
                    continue
                break
            for fil in self.layers[index]:
                fil.execute()
            # If any filter has outputted any atom, call the on_atom_output method
            if on_atom_output != None:
                for iterator in last_output_iterators:
                    while(iterator.has_next()):
                        on_atom_output(next(iterator))
