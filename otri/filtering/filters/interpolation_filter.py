from ..filter import Filter, StreamIter, Stream, Collection
from ..stream import Stream
from datetime import datetime, timedelta

TIMEDELTA_DICT : dict = {
        "seconds" : timedelta(seconds=1),
        "minutes" : timedelta(minutes=1),
        "hours" : timedelta(hours=1),
        "days" : timedelta(days=1)
    }

class InterpolationFilter(Filter):
    '''

    Input:
        Oredered by datetime atoms.
    Output:
        Atoms interpolated for the given target interval
    '''

    def __init__(self, input_stream: Stream, keys_to_change: Collection[str], target_interval: str = "minutes"):
        '''
        Parameters:
            input_stream : Stream
                Input stream.
            values_to_change : Collection[str]
                Collection of keys to update when calculating interpolation. Will be the only keys of the atoms (with datetime too).
            target_interval : str
                The maximum interval between successive atoms.
                Could be "seconds", "minutes", "hours", "days".
        '''
        super().__init__(input_streams=[input_stream],
                         input_streams_count=1, output_streams_count=1)
        self.input_stream_iter = input_stream.__iter__()
        self.output_stream = self.get_output_stream(0)
        self.target_interval = target_interval
        self.keys_to_change = keys_to_change
        self.timeunit = self.__timedelta_from_interval(
            interval=target_interval)
        self.atom_buffer = None

    def execute(self):
        '''
        Waits for two atoms and interpolates the given dictionary values.
        '''
        if(self.input_stream_iter.has_next()):
            atom = next(self.input_stream_iter)
            if(self.atom_buffer == None):
                # Do nothing, just save the atom for the next
                self.atom_buffer = atom
            else:
                self.create_missing_atoms(atom)
        elif(self.input_streams[0].is_finished()):
            # Empty the atom_buffer (should contain one atom)
            if(self.atom_buffer != None):
                self.output_stream.append(self.atom_buffer)
                self.atom_buffer = None
            self.output_stream.close()

    def create_missing_atoms(self, atom: dict):
        atom1_datetime = datetime.strptime(
            self.atom_buffer['datetime'], "%Y-%m-%d %H:%M:%S.%f")
        atom2_datetime = datetime.strptime(
            atom['datetime'], "%Y-%m-%d %H:%M:%S.%f")
        new_atom_datetime = atom1_datetime + self.timeunit
        self.output_stream.append(self.atom_buffer)

        while(new_atom_datetime < atom2_datetime):
            new_atom = {}
            new_atom['datetime'] = new_atom_datetime.strftime(
                "%Y-%m-%d %H:%M:%S.%f")[:-3]
            progress = (new_atom_datetime - atom1_datetime).total_seconds() / \
                (atom2_datetime - atom1_datetime).total_seconds()
            for key in self.keys_to_change:
                new_atom[key] = self.atom_buffer[key] + \
                    (atom[key] - self.atom_buffer[key]) * progress
            self.output_stream.append(new_atom)

            new_atom_datetime = new_atom_datetime + self.timeunit
        self.atom_buffer = atom

    def __timedelta_from_interval(self, interval: str) -> timedelta:
        '''
        Returns the unit timedelta given the interval.

        Parameters:
            interval : str
                The interval to use to calculate the number of datetimes between the two give atoms'.
                Could be "seconds", "minutes", "hours", "days".
        Returns:
            datetime.timedelta
        Raises:
            ValueError if the interval is not supported
        '''
        value = TIMEDELTA_DICT.get(interval, None)
        if(value == None):
            raise ValueError("Interval {} is not supported".format(interval))
        return value
