from ..filter import Filter, Stream, Sequence, Mapping, Any
from typing import Callable, Collection


class PhaseFilter(Filter):
    '''
    Applies an operation to the i atom's keys and i+c atom's keys.

    Input:
        Single stream ordered by datetime
    Output:
        Single stream containing n - c atoms, whose fields are the
        result of the given operation between the atoms at positions
        i and i+c in the input Stream. Such fields are the only fields
        of the output data.
    '''

    def __init__(self, inputs: str, outputs: str, keys_to_change: Mapping[str, Callable[[Any, Any], Any]], distance: int):
        '''
        Parameters:
            inputs : str
                Input stream name. All atoms in the Stream will be treated as if they had all of the
                keys in `keys_to_change`.
            outputs : str
                Output stream name.
            keys_to_change : Mapping[str, Callable]
                A mapping of each key that needs to be modified along with the operation
                to use. Such operation should take two parameters and output a coherent value.
                These keys should be in all of the atoms of the Stream, to allow a proper output
                Stream. These will be the only keys in the output atoms.
            distance : int
                Distance in number of atoms to calculate a[i] * a[i+c]
        '''
        super().__init__(
            inputs=[inputs],
            outputs=[outputs],
            input_count=1,
            output_count=1
        )
        self.__keys = keys_to_change
        self.__distance = distance
        self.__atoms_buffer = list()
        self.__counter = 0

    def setup(self, inputs: Sequence[Stream], outputs: Sequence[Stream], state: Mapping[str, Any]):
        '''
        Used to save references to streams and reset variables.
        Called once before the start of the execution in FilterNet.

        Parameters:
            inputs, outputs : Sequence[Stream]
                Ordered sequence containing the required input/output streams gained from the FilterNet.
            state : Mapping[str, Any]
                Dictionary containing states to output.
        '''
        self.__input = inputs[0]
        self.__input_iter = iter(inputs[0])
        self.__output = outputs[0]

    def execute(self):
        '''
        Pops atoms from the input Stream and places them in an internal buffer.
        When the internal buffer reaches the size of the requested distance we
        produce a new output atom whose fields are the results of the Callables
        in the `keys_to_change` init parameter. The last `distance` parameters
        are discarded.
        '''
        if(self.__output.is_closed()):
            return

        if(self.__input_iter.has_next()):
            if(len(self.__atoms_buffer) < self.__counter + 1):
                self.__atoms_buffer.append(next(self.__input_iter))
            else:
                atom_1 = self.__atoms_buffer[self.__counter]
                atom_2 = next(self.__input_iter)
                mul_atom = dict()
                for k in self.__keys.keys():
                    mul_atom[k] = self.__keys[k](atom_1[k], atom_2[k])
                self.__atoms_buffer[self.__counter] = atom_2
                self.__output.append(mul_atom)
            self.__counter = (self.__counter + 1) % self.__distance
        elif(self.__input.is_closed()):
            self.__output.close()


class PhaseMulFilter(PhaseFilter):
    '''
    Multiplies i atom's given keys with i+c atom's keys
    Input:
        Single stream ordered by datetime
    Output:
        Single stream containing n - c atoms
    '''

    def __init__(self, inputs: str, outputs: str, keys_to_change: Collection[str], distance: int):
        '''
        Parameters:
            inputs : str
                Input stream name.
            outputs : str
                Output stream name.
            keys_to_change : Collection[str]
                Collection of keys whom values will be multiplied.
            distance : int
                Distance in number of atoms to calculate a[i] * a[i+c]
        '''
        super().__init__(
            inputs=inputs,
            outputs=outputs,
            keys_to_change={k: lambda x, y: x * y for k in keys_to_change},
            distance=distance
        )


class PhaseDeltaFilter(PhaseFilter):
    '''
    Subtracts i atom's given keys with i+c atom's keys
    Input:
        Single stream ordered by datetime
    Output:
        Single stream containing n - c atoms
    '''

    def __init__(self, inputs: str, outputs: str, keys_to_change: Collection[str], distance: int):
        '''
        Parameters:
           inputs : str
                Input stream name.
            outputs : str
                Output stream name.
            keys_to_change : Collection[str]
                Collection of keys whom deltas will be calculated.
            distance : int
                Distance in number of atoms for which to calculate a[i] - a[i+c]
        '''
        super().__init__(
            inputs=inputs,
            outputs=outputs,
            keys_to_change={k: (lambda x, y: x - y) for k in keys_to_change},
            distance=distance
        )
