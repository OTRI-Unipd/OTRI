from ..filter import Filter, Stream, Sequence, Mapping, Any
from datetime import timedelta, datetime, timezone
from ...utils import time_handler as th


class GroupHandler:

    def setup(self):
        '''
        Method called when the start method of the group filter is called. Use it to initalise or reset varaibiles.
        '''
        pass

    def group(self, atoms: Sequence[Mapping]) -> Mapping:
        '''
        Called when a group of atoms is gathered in the group filter. Builds the grouped atom.\n

        Parameters:\n
            atoms : Sequence[Mapping]
                List of atoms that has the same datetime on the target resolution.\n
        Retruns: a single atom.\n
        '''
        raise NotImplementedError("group function must be implemented by GroupHandler")


class GroupFilter(Filter):
    '''
    Groups atoms to a lower datetime resolution.\n

    Inputs:
        Oredered by datetime atoms.\n
    Outputs:
        Atoms with lower resolution.\n
    '''

    def __init__(self, inputs: str, outputs: str, group_handler: GroupHandler, target_resolution: timedelta = timedelta(days=1), datetime_key: str = "datetime"):
        '''
        Parameters:\n
             input, output : Sequence[str]
                Name for input/output streams.\n
            group_handler : GroupHandler
                Classe used to group atoms.\n
            target_resolution : timedelta
                Target resolution, must be lower than current resolution. Valid values are: 1,2,3,4,5,6,8,10,12,15,30 seconds or minutes 1,2,3,4,6,8,12 hours 1 day.\n
            datetime_key : str
                Key name for the datetime value.\n
        '''
        super().__init__(
            inputs=[inputs],
            outputs=[outputs],
            input_count=1,
            output_count=1
        )
        self.__resolution = target_resolution
        self.__number_res = self._timedelta_to_number(target_resolution)
        self.__datetime_key = datetime_key
        self.__group_handler = group_handler

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
        self.__cur_dt = datetime(1980, 1, 1, 1, 1)
        self.__buffer = list()
        self.__group_handler.setup()

    def _on_data(self, data: Any, index: int):
        '''
        Waits until all atom of the target resolution have been processed then outputs the result atom.
        '''
        # Check if the atom passed the resolution timespan
        next_dt = self.__cur_dt + self.__resolution
        if data[self.__datetime_key] >= th.datetime_to_str(next_dt):
            if self.__buffer:
                # Passed the timespan of the resolution, calculate grouped atom
                grouped_atom = self.__group_handler.group(self.__buffer)
                grouped_atom[self.__datetime_key] = th.datetime_to_str(self.__cur_dt)
                self._push_data(data=grouped_atom)
                self.__buffer.clear()
            # Find the remainder of the division of time/resolution
            data_number_dt = self._datetime_to_number(th.str_to_datetime(data[self.__datetime_key]))
            remainder = data_number_dt % self.__number_res
            # Use the remainder to find the closest lower value of the resolution eg 08:15 res 1h = 08:15 - 00:15 = 08:00
            self.__cur_dt = self._number_to_datetime(data_number_dt - remainder)
        # Update buffer
        self.__buffer.append(data)

    def _on_inputs_closed(self):
        '''
        All of the inputs are closed and no more data is available.
        The filter should empty itself and close all of the output streams.
        '''
        if self.__buffer:
            grouped_atom = self.__group_handler.group(self.__buffer)
            grouped_atom[self.__datetime_key] = th.datetime_to_str(self.__cur_dt)
            self._push_data(data=grouped_atom)
        super()._on_inputs_closed()

    def _datetime_to_number(self, dt: datetime) -> int:
        '''
        Converts a datetime to a format covenient for the calculation of resolution times.
        '''
        return int("{:04d}{:02d}{:02d}{:02d}{:02d}{:02d}{:03d}".format(dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second, int(dt.microsecond/1000)))

    def _number_to_datetime(self, number: int) -> datetime:
        '''
        Converts a number to a datetime, eg. "20200815081520000"-> "2020-08-15 08:15:20.000"
        '''
        numberstr = str(number)
        year = int(numberstr[:4])
        month = int(numberstr[4:6])
        day = int(numberstr[6:8])
        hours = int(numberstr[8:10])
        minutes = int(numberstr[10:12])
        seconds = int(numberstr[12:14])
        micros = int(numberstr[14:17]) * 1000
        return datetime(
            year=year,
            month=month,
            day=day,
            hour=hours,
            minute=minutes,
            second=seconds,
            microsecond=micros,
            tzinfo=timezone.utc
        )

    def _timedelta_to_number(self, td: timedelta) -> int:
        '''
        Converts a datetime to a number, eg. "2020-08-15 08:15:20.000" -> "20200815081520000"
        '''
        return int("{:02d}{:02d}{:02d}{:02d}000".format(td.days, int(td.seconds//3600), int((td.seconds/60) % 60), int(td.seconds % 60)))


class TimeSeriesGroupHandler(GroupHandler):

    def group(self, atoms: Sequence[Mapping]) -> Mapping:
        t_high = float("-inf")
        t_low = float("inf")
        t_volume = 0
        # Calculate new high low and volume values
        for atom in atoms:
            if float(atom['high']) > t_high:
                t_high = float(atom['high'])
            if float(atom['low']) < t_low:
                t_low = float(atom['low'])
            try:
                t_volume += int(atom['volume'])
            except KeyError:
                # Missing volume key, will skip
                continue
        # Build the atom
        return {
            'open': str(atoms[0]['open']),
            'close': str(atoms[len(atoms)-1]['close']),
            'high': str(t_high),
            'low': str(t_low),
            'volume': str(t_volume)
        }


class TimeseriesGroupFilter(GroupFilter):

    def __init__(self, inputs: str, outputs: str, target_resolution: timedelta = timedelta(days=1), datetime_key="datetime"):
        '''
        Parameters:\n
             input, output : Sequence[str]
                Name for input/output streams.\n
            target_resolution : timedelta
                Target resolution, must be lower than current resolution. Valid values are: 1,2,3,4,5,6,8,10,12,15,30 seconds or minutes 1,2,3,4,6,8,12 hours 1 day.\n
            datetime_key : str
                Key name for the datetime value.\n
        '''
        handler = TimeSeriesGroupHandler()
        super().__init__(inputs=inputs, outputs=outputs, target_resolution=target_resolution, datetime_key=datetime_key, group_handler=handler)
