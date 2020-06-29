from ..filter import Filter, Stream, Sequence, Any, Mapping
from typing import Collection
from datetime import timedelta
from ...utils import time_handler as th

TIMEDELTA_DICT: dict = {
    "seconds": timedelta(seconds=1),
    "minutes": timedelta(minutes=1),
    "hours": timedelta(hours=1),
    "days": timedelta(days=1)
}


class InterpolationFilter(Filter):
    '''
    Interpolates value between two stream atoms if their time difference is greater than a given maximum interval.
    The resulting atoms will have given values interpolated.
    Input:
        Oredered by datetime atoms.
    Output:
        Atoms interpolated for the given target interval
    '''

    def __init__(self, inputs: str, outputs: str, keys_to_interp: Collection[str], target_interval: str = "minutes"):
        '''
        Parameters:
            inputs : str
                Input stream name.
            outputs : str
                Output stream name.
            keys_to_interp : Collection[str]
                Collection of keys to update when calculating interpolation. Will be the only keys of the atoms (with datetime too).
            target_interval : str
                The maximum interval between successive atoms.
                Could be "seconds", "minutes", "hours", "days".
        '''
        super().__init__(
            inputs=[inputs],
            outputs=[outputs],
            input_count=1,
            output_count=1
        )
        self.target_interval = target_interval
        self.keys_to_interp = keys_to_interp
        self.timeunit = InterpolationFilter.__timedelta_from_interval(
            interval=target_interval)
        self.atom_buffer = None

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
        Waits for two atoms and interpolates the given dictionary values.
        '''
        if self.__output.is_closed():
            return

        if self.__input_iter.has_next():
            atom = next(self.__input_iter)
            if(self.atom_buffer == None):
                # Do nothing, just save the atom for the next
                self.atom_buffer = atom
            else:
                self.__create_missing_atoms(atom, self.__output)
        elif(self.__input.is_closed()):
            # Empty the atom_buffer (should contain one atom)
            if(self.atom_buffer != None):
                self.__output.append(self.atom_buffer)
                self.atom_buffer = None
            self.__output.close()

    def __create_missing_atoms(self, atom: dict, output: Stream):
        '''
        Pushes into the output stream the current self.atom_buffer and all the interpolated atoms between that and the give atom.
        '''
        atom1_datetime = th.str_to_datetime(self.atom_buffer['datetime'])
        atom2_datetime = th.str_to_datetime(atom['datetime'])
        atom12_dt_diff = (atom2_datetime - atom1_datetime).total_seconds()
        new_atom_datetime = atom1_datetime + self.timeunit

        # Place the current atom_buffer into the output
        output.append(self.atom_buffer)

        while(new_atom_datetime < atom2_datetime):
            new_atom = {}
            new_atom['datetime'] = th.datetime_to_str(new_atom_datetime)
            progress = (new_atom_datetime -
                        atom1_datetime).total_seconds() / atom12_dt_diff
            for key in self.keys_to_interp:
                new_atom[key] = self.atom_buffer[key] + \
                    (atom[key] - self.atom_buffer[key]) * progress
            output.append(new_atom)

            new_atom_datetime = new_atom_datetime + self.timeunit
        self.atom_buffer = atom

    @staticmethod
    def __timedelta_from_interval(interval: str) -> timedelta:
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
