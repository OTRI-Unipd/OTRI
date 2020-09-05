from ..filter import Filter, Sequence, Any
from typing import Collection
from datetime import timedelta, datetime, time, timezone
from ...utils import time_handler as th
from ...utils import logger as log
from numpy import interp


class InterpolationFilter(Filter):
    '''
    Interpolates value between two stream atoms forcing a constant frequency between them.\n
    The resulting atoms will have given values interpolated.\n

    Inputs:\n
        Oredered by datetime atoms.\n
    Outputs:\n
        Atoms interpolated at the desired frequency.\n
    '''

    def __init__(self, inputs: str, outputs: str, interp_keys: Collection[str], constant_keys: Collection[str] = []):
        '''
        Parameters:\n
            inputs : str\n
                Input stream name.\n
            outputs : str\n
                Output stream name.\n
            interp_keys : Collection[str]\n
                Collection of keys to update when calculating interpolation. Will be the only keys of the atoms (with datetime too).\n
            constant_keys : Collection[str]\n
                Collection of keys that remain constant between atoms. When choosing between two values it'll choose the value from earlier atom.\n
        '''
        super().__init__(
            inputs=[inputs],
            outputs=[outputs],
            input_count=1,
            output_count=1
        )
        self._interp_keys = interp_keys
        self._constant_keys = constant_keys
        self.atom_buffer = None

    def _on_data(self, data, index):
        '''
        Waits for two atoms and interpolates the given dictionary values.
        '''
        if(self.atom_buffer is None):
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
    Interpolates value between two stream atoms forcing a constant frequency between them.
    The resulting atoms will have given values interpolated.\n
    Can only use seconds as period time, works for intraday atoms, not for daily or weekly atoms.\n

    Inputs:\n
        Oredered by datetime atoms.\n
    Outputs:\n
        Atoms interpolated at the desired frequency.\n
    '''

    def __init__(self, inputs: str, outputs: str, interp_keys: Collection[str], constant_keys: Collection[str] = [], target_gap_seconds: int = 60, working_hours: tuple = (time(hour=8, tzinfo=timezone.utc), time(hour=20, tzinfo=timezone.utc))):
        '''
        Parameters:\n
            inputs : str\n
                Input stream name.\n
            outputs : str\n
                Output stream name.\n
            interp_keys : Collection[str]\n
                Collection of keys to update when calculating interpolation. Will be the only keys of the atoms (with datetime too).\n
            constant_keys : Collection[str]\n
                Collection of keys that remain constant between atoms. When choosing between two values it'll choose the value from earlier atom.\n
            target_gap_seconds : str\n
                The target gap between atoms in seconds.\n
            working_hours : tuple(start, end)\n
                Tuple containing datetime.time for start hour minutes and seconds and end time. Values will be calculated inside these working hours only.
                The start time must be earlier than end time by at least one target_gap_seconds seconds\n
        '''
        super().__init__(
            inputs=inputs,
            outputs=outputs,
            interp_keys=interp_keys,
            constant_keys=constant_keys
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

        while(self.__instant_iterator <= (A_datetime.timetz()) and self.__instant_iterator <= self.__working_hours[1]):
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
        while(self.__instant_iterator <= B_datetime.timetz() and self.__instant_iterator <= self.__working_hours[1]):
            interp_instants.append(th.datetime_to_epoch(datetime.combine(self.cur_day, self.__instant_iterator)))
            self.__instant_iterator = th.sum_time(self.__instant_iterator, self.__gap_datetime)

        output_atoms = []
        interp_values = {}

        # Interpolate values of every key
        for key in self._interp_keys:
            try:
                interp_values[key] = interp(
                    x=interp_instants,
                    xp=[A_epoch, B_epoch],
                    fp=[float(self.atom_buffer[key]), float(B[key])]
                )
            except KeyError:
                print("Atom doesn't contain key {}: {}".format(key, self.atom_buffer))
            except TypeError:
                print("Atom's key {} is not a number: {}".format(key, self.atom_buffer))

        # Generate intermediate atoms
        for i in range(len(interp_instants)):
            atom = {}
            atom['datetime'] = th.datetime_to_str(datetime.utcfromtimestamp(interp_instants[i]).replace(tzinfo=timezone.utc))
            for key in self._interp_keys:
                atom[key] = interp_values[key][i]
            for key in self._constant_keys:
                atom[key] = self.atom_buffer[key]
            output_atoms.append(atom)

        return output_atoms
