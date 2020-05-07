from ..filter import Filter, StreamIter, Stream, Collection
from ..stream import Stream
from datetime import datetime

class InterpolationFilter(Filter):

    def __init__(self, input_stream: Stream, values_to_change : Collection[str], target_interval : str ="1m"):
        '''
    	Parameters:
            input_stream : Stream
                Input stream.
            values_to_change : Collection[str]
                Collection of keys to update when calculating interpolation. Will be the only keys of the atoms (with datetime too).
            target_interval : str
                The maximum interval between successive atoms.
                Could be "1s", "1m", "1h" (for now)
        '''
        super().__init__(input_streams=[input_stream], input_streams_count=1, output_streams_count=1)
        self.input_stream_iter = input_stream.__iter__()
        self.output_stream = self.get_output_stream(0)

    def execute(self):
        '''
        Waits for two atoms and interpolates the given dictionary values
        '''
        if(self.input_stream_iter.has_next()):
            pass
        elif(self.input_streams[0].is_finished()):
            self.output_stream.close()
    
    def calc_missing_atoms(self, atom1, atom2, interval):
        # d1 = datetime.strptime(atom1['datetime'], "%Y-%m-%d %H:%M:%S.%f")
        # d2 = datetime.strptime(atom2['datetime'], "%Y-%m-%d %H:%M:%S.%f")
        # switch(interval): case "1s" return (d2-d1).seconds
        return 5