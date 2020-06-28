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

    Inputs:
        Oredered by datetime atoms.
    Outputs:
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
        self.timeunit = InterpolationFilter.__timedelta_from_interval(interval=target_interval)
        self.atom_buffer = None

    def _on_data(self, data, index):
        '''
        Waits for two atoms and interpolates the given dictionary values.
        '''
        if(self.atom_buffer == None):
            # Do nothing, just save the atom for the next iteration
            self.atom_buffer = data
        else:
            output_atoms = self.__create_missing_atoms(data, self.__output)
            for atom in output_atoms:
                self._push_data(atom)

    def _on_inputs_closed(self):
        '''
        Pushes out the atom in the buffer and closes the outputs
        '''
        if(self.atom_buffer != None):
            self._push_data(self.atom_buffer)
            self.atom_buffer = None
        self._get_output().close()

    def __create_missing_atoms(self, atom: dict, output: Stream) -> Any:
        '''
        Pushes into the output stream the current self.atom_buffer and all the interpolated atoms between that and the give atom.
        '''
        atom1_datetime = th.str_to_datetime(self.atom_buffer['datetime'])
        atom2_datetime = th.str_to_datetime(atom['datetime'])
        atom12_dt_diff = (atom2_datetime - atom1_datetime).total_seconds()
        new_atom_datetime = atom1_datetime + self.timeunit
        output_atoms = list()
        # Place the current atom_buffer into the output
        output_atoms.append(self.atom_buffer)

        while(new_atom_datetime < atom2_datetime):
            new_atom = {}
            new_atom['datetime'] = th.datetime_to_str(new_atom_datetime)
            progress = (new_atom_datetime -
                        atom1_datetime).total_seconds() / atom12_dt_diff
            for key in self.keys_to_interp:
                new_atom[key] = self.atom_buffer[key] + (atom[key] - self.atom_buffer[key]) * progress
            output_atoms.append(new_atom)

            new_atom_datetime = new_atom_datetime + self.timeunit
        self.atom_buffer = atom
        return output_atoms

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
