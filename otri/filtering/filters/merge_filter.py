from ..filter import Filter, Stream, Sequence, Mapping, Any


class SequentialMergeFilter(Filter):
    '''
    Sequentially merges elements from multiple streams into one single output.
    
    Input:
        Multiple streams.
    Output:
        A single stream containing data read sequentially (all of stream 1, then all of stream 2 and so on).
    '''

    def __init__(self, inputs: Sequence[str], outputs: str):
        '''
        Parameters:
             input : Sequence[str]
                Name for input streams.
            output : str
                Name for output stream.
        '''
        super().__init__(
            inputs=inputs,
            outputs=[outputs],
            input_count=len(inputs),
            output_count=1)

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
        self.__inputs = inputs
        self.__output = outputs[0]

    def execute(self):
        '''
        Pops elements from the input streams sequentially (all of stream 0 then all of stream 1 and so on) and places them into the single output stream.
        '''
        if(self.__output.is_closed()):
            return
        # Extracts input data sequentially from each input filter
        for input_str in self.__inputs:
            if iter(input_str).has_next():
                self.__output.append(next(iter(input_str)))
                return
        # Checks if there is anymore data
        for input_str in self.__inputs:
            if not input_str.is_closed():
                return
            
        # If we get here it means that all of the input streams are closed, hence we define the output as closed
        self.__output.close()
