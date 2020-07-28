from ..filter import Filter, Stream, Sequence, Any, Mapping
from typing import Collection
from datetime import timedelta, datetime, time, date
from ...utils import time_handler as th
from ...utils import logger as log
from numpy import interp


class InterpolationFilter(Filter):
    '''
    Interpolates value between two stream atoms if their time difference is greater than a given maximum interval.
    The resulting atoms will have given values interpolated.

    Inputs:
        Oredered by datetime atoms.
    Outputs:
        Atoms interpolated at the desired frequency.
    '''

    def __init__(self, inputs: str, outputs: str, keys_to_interp: Collection[str]):
        '''
        Parameters:
            inputs : str
                Input stream name.
            outputs : str
                Output stream name.
            keys_to_interp : Collection[str]
                Collection of keys to update when calculating interpolation. Will be the only keys of the atoms (with datetime too).
            target_gap_seconds : str
                The target gap between atoms in seconds.
            working_hours : tuple(start, end)
                Tuple containing datetime for start hour minutes and seconds and end time. Values will be calculated inside these working hours only.
        '''
        super().__init__(
            inputs=[inputs],
            outputs=[outputs],
            input_count=1,
            output_count=1
        )
        self.keys_to_interp = keys_to_interp
        self.atom_buffer = None

    def _on_data(self, data, index):
        '''
        Waits for two atoms and interpolates the given dictionary values.
        '''
        if(self.atom_buffer == None):
            # Do nothing, just save the atom for the next iteration
            self.atom_buffer = data
        else:
            output_atoms = self._create_atoms(data)
            for atom in output_atoms:
                self._push_data(atom)

    def _create_atoms(self, B: dict) -> Sequence[dict]:
        raise NotImplementedError()


class IntradayInterpolationFilter(InterpolationFilter):
    '''
    Interpolates value between two stream atoms if their time difference is greater than a given maximum interval.
    The resulting atoms will have given values interpolated.\n

    Inputs:\n
        Oredered by datetime atoms.\n
    Outputs:\n
        Atoms interpolated at the desired frequency.\n
    '''

    def __init__(self, inputs: str, outputs: str, keys_to_interp: Collection[str], target_gap_seconds: int = 60, working_hours: tuple = (time(hour=8), time(hour=20))):
        '''
        Parameters:\n
            inputs : str\n
                Input stream name.\n
            outputs : str\n
                Output stream name.\n
            keys_to_interp : Collection[str]\n
                Collection of keys to update when calculating interpolation. Will be the only keys of the atoms (with datetime too).\n
            target_gap_seconds : str\n
                The target gap between atoms in seconds.\n
            working_hours : tuple(start, end)\n
                Tuple containing datetime.time for start hour minutes and seconds and end time. Values will be calculated inside these working hours only.
                The start time must be earlier than end time by at least one target_gap_seconds seconds\n
        '''
        super().__init__(
            inputs=inputs,
            outputs=outputs,
            keys_to_interp=keys_to_interp
        )
        self.__gap_datetime = timedelta(seconds=target_gap_seconds)
        self.__working_hours = working_hours
        # Iterates through time of the day and resets on a new day
        self.__instant_iterator = working_hours[0]

    def _create_atoms(self, B: dict) -> Any:
        '''
        Pushes into the output stream the current self.atom_buffer and all the interpolated atoms between that and the give atom.\n
        '''
        A_datetime = th.str_to_datetime(self.atom_buffer['datetime'])
        try: 
            B_datetime = th.str_to_datetime(B['datetime'])
        except KeyError as e:
            log.e("unable to parse atom: {}, error: {}".format(B, e))
        A_epoch = th.datetime_to_epoch(A_datetime)
        B_epoch = th.datetime_to_epoch(B_datetime)

        interp_instants = []

        # Interpolate from the starting hour for the first atom
        if not hasattr(self, "cur_day"):
            self.cur_day = A_datetime.date()
            self.__instant_iterator = self.__working_hours[0]

        while(self.__instant_iterator <= th.datetime_to_time(A_datetime) and self.__instant_iterator <= self.__working_hours[1]):
            interp_instants.append(th.datetime_to_epoch(datetime.combine(A_datetime.date(), self.__instant_iterator)))
            self.__instant_iterator = th.sum_time(self.__instant_iterator, self.__gap_datetime)
        
        # Interpolate between the two atoms
        while(self.cur_day < B_datetime.date()):
            while(self.__instant_iterator <= self.__working_hours[1]):
                interp_instants.append(th.datetime_to_epoch(datetime.combine(self.cur_day, self.__instant_iterator)))
                self.__instant_iterator = th.sum_time(self.__instant_iterator, self.__gap_datetime)
            self.cur_day += timedelta(days=1)
            self.__instant_iterator = self.__working_hours[0]

        # Reached the B day
        while(self.__instant_iterator <= th.datetime_to_time(B_datetime) and self.__instant_iterator <= self.__working_hours[1]):
            interp_instants.append(th.datetime_to_epoch(datetime.combine(self.cur_day, self.__instant_iterator)))
            self.__instant_iterator = th.sum_time(self.__instant_iterator, self.__gap_datetime)
            
        output_atoms = []
        interp_values = {}
        for key in self.keys_to_interp:
            interp_values[key] = interp(
                x = interp_instants,
                xp = [A_epoch, B_epoch],
                fp = [float(self.atom_buffer[key]), float(B[key])]
            )
        
        for i in range(len(interp_instants)):
            atom = {}
            atom['datetime'] = th.datetime_to_str(datetime.utcfromtimestamp(interp_instants[i]))
            for key in self.keys_to_interp:
                atom[key] = interp_values[key][i]
            output_atoms.append(atom)

        return output_atoms
