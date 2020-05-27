from ..filter import Filter, Stream, Sequence


class SequentialMergeFilter(Filter):
    '''
    Sequentially merges elements from multiple streams into one single output.
    
    Input:
        Multiple streams.
    Output:
        A single stream containing data read sequentially (all of stream 1, then all of stream 2 and so on).
    '''

    def __init__(self, input: Sequence[str], output: str):
        '''
        Parameters:
             input : Sequence[str]
                Name for input streams.
            output : str
                Name for output stream.
        '''
        super().__init__(
            input=input,
            output=[output],
            input_count=len(input),
            output_count=1)

    def execute(self, inputs : Sequence[Stream], outputs : Sequence[Stream]):
        '''
        Pops elements from the input streams sequentially (all of stream 0 then all of stream 1 and so on) and places them into the single output stream.

        Parameters:
            inputs, outputs : Sequence[Stream]
                Ordered sequence containing the required input/output streams gained from the FilterList.
        '''
        if(outputs[0].is_closed()):
            return
        # Extracts input data sequentially from each input filter
        for input_str in inputs:
            if iter(input_str).has_next():
                outputs[0].append(next(iter(input_str)))
                return
            elif not input_str.is_closed():
                return
            
        # If we get here it means that all of the input streams are closed, hence we define the output as closed
        outputs[0].close()
