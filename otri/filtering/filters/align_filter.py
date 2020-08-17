
__author__ = "Luca Crema <lc.crema@hotmail.com>"

from ..filter import Filter, Sequence, Any, Mapping, Stream
from ...utils import time_handler as th


class AlignFilter(Filter):
    '''
    Aligns data from multiple inputs, outputting only when the atoms have the same datetime.

    Inputs:
        Oredered by datetime atoms.\n
    Outputs:
        Atoms aligned at the same datetime.\n
    '''

    def __init__(self, inputs: Sequence[str], outputs: Sequence[str], datetime_key: str = "datetime"):
        '''
        Parameters:\n
             input, output : Sequence[str]
                Name for input/output streams.\n
            datetime_key : str
                Key name for the datetime value to align.\n
        '''
        super().__init__(
            inputs=inputs,
            outputs=outputs,
            input_count=len(inputs),
            output_count=len(outputs)
        )
        self.__datetime_key = datetime_key

    def setup(self, inputs: Sequence[Stream], outputs: Sequence[Stream], state: Mapping[str, Any]):
        '''
        Used to save references to streams and reset variables.\n
        Called once before the start of the execution in FilterNet.\n

        Parameters:\n
            inputs, outputs : Sequence[Stream]
                Ordered sequence containing the required input/output streams gained from the FilterNet.\n
            state : Mapping[str, Any]
                Dictionary containing states to output.\n
        '''
        # Call superclass setup
        super().setup(inputs, outputs, state)

        self.__inputs_len = len(inputs)
        self.__atoms = [None] * self.__inputs_len
        self.__next_stream = 0

    def _on_data(self, data: Any, index: int):
        '''
        Keeps waiting for atoms until they're all aligned, then outputs them in the right stream.
        '''
        self.__atoms[index] = data
        for i, atom in enumerate(self.__atoms):
            if atom is not None:
                if self.__is_earlier(atom):
                    self.__next_stream = i
                    return
        # No atom is earlier than the others, we can proceed to the next one
        self.__next_stream = (index + 1) % self.__inputs_len
        # If we still have None atoms we have to go on
        for atom in self.__atoms:
            if atom is None:
                return
        # If we have all of the atoms aligned we can proceed to output
        for i in range(len(self.__atoms)):
            self._push_data(self.__atoms[i], i)
            self.__atoms[i] = None

    def __is_earlier(self, atom: dict) -> bool:
        '''
        Checks if the atom has an earlier datetime than every other atom
        '''
        for t_atom in self.__atoms:
            if t_atom is not None:
                if atom[self.__datetime_key] < t_atom[self.__datetime_key]:
                    return True
        return False

    def _input_check_order(self) -> Sequence[int]:
        '''
        Defines the order for the inputs to be checked.
        '''
        return [self.__next_stream]
