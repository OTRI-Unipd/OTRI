from ..filter import Filter, StreamIter, Stream, Collection
from ..stream import Stream
from datetime import datetime, timedelta


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
                n_missing_atoms = self.calc_missing_atoms(
                    atom1=self.atom_buffer, atom2=atom, interval=self.target_interval)
                self.output_stream.append(self.atom_buffer)
                buffer_atom_datetime = datetime.strptime(self.atom_buffer['datetime'], "%Y-%m-%d %H:%M:%S.%f")
                for i in range(1, n_missing_atoms+1):
                    new_atom = {}
                    new_atom['datetime'] = (buffer_atom_datetime + (self.timeunit * i)).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
                    progress = (i/(n_missing_atoms+1))
                    for key in self.keys_to_change:
                        new_atom[key] = self.atom_buffer[key] + \
                            (atom[key] - self.atom_buffer[key]) * progress
                    self.output_stream.append(new_atom)
                self.atom_buffer = atom
        elif(self.input_streams[0].is_finished()):
            # Empty the atom_buffer (should contain one atom)
            if(self.atom_buffer != None):
                self.output_stream.append(self.atom_buffer)
                self.atom_buffer = None
            self.output_stream.close()

    def calc_missing_atoms(self, atom1: dict, atom2: dict, interval: str):
        '''
        Calculates the number of missing atoms between two given atoms.

        Parameters:
            atom1 : dict
                Atom containing 'datetime' field.
            atom2 : dict
                Atom containing 'datetime' field. Its datetime must be greater that atom1's.
            interval : str
                The interval to use to calculate the number of datetimes between the two give atoms'.
                Could be "seconds", "minutes", "hours", "days".
        Raises:
            ValueError if the interval is not supported or the two given atoms are not ordered by datetime or have the same datetime 
        '''
        d1 = datetime.strptime(atom1['datetime'], "%Y-%m-%d %H:%M:%S.%f")
        d2 = datetime.strptime(atom2['datetime'], "%Y-%m-%d %H:%M:%S.%f")
        diff = (d2 - d1)
        value = None
        if(interval == "seconds"):
            value = diff.seconds - 1
        if(interval == "minutes"):
            value = round(diff.seconds/60) - 1
        if(interval == "hours"):
            value = round(diff.seconds/3600) - 1
        if(interval == "days"):
            value = diff.days() - 1
        if(value == None):
            raise ValueError("Interval {} is not supported".format(interval))
        if(value < 0):
            raise ValueError(
                "The atom list is not datetime-ordered or two atoms have the same datetime:\n{}\n{}".format(atom1, atom2))
        return value

    def __timedelta_from_interval(self, interval: str):
        '''
        Returns the unit timedelta given the interval.

        Parameters:
            interval : str
                The interval to use to calculate the number of datetimes between the two give atoms'.
                Could be "seconds", "minutes", "hours", "days".
        Raises:
            ValueError if the interval is not supported
        '''
        if(interval == "seconds"):
            return timedelta(seconds=1)
        if(interval == "minutes"):
            return timedelta(minutes=1)
        if(interval == "hours"):
            return timedelta(hours=1)
        if(interval == "days"):
            return timedelta(days=1)
        raise ValueError("Interval {} is not supported".format(interval))
