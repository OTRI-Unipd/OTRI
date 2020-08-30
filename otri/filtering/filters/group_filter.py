from ..filter import Filter, Stream, Sequence, Mapping, Any
from datetime import timedelta, datetime, timezone
from ...utils import time_handler as th


class GroupFilter(Filter):
    '''
    Groups price atoms to a lower resolution.\n

    Inputs:
        Oredered by datetime atoms.\n
    Outputs:
        Atoms with lower resolution\n
    '''

    def __init__(self, inputs: str, outputs: str, resolution: timedelta = timedelta(days=1), datetime_key: str = "datetime", volume_key: str = None):
        '''
        Parameters:\n
             input, output : Sequence[str]
                Name for input/output streams.\n
            resolution : timedelta
                Target resolution, must be lower than current resolution. Valid values are: 1,2,3,4,5,6,8,10,12,15,30 seconds or minutes 1,2,3,4,6,8,12 hours 1 day.\n
            datetime_key : str
                Key name for the datetime value.\n
            volume_key : str
                Key name for the volume value. If None it won't group volume value.\n
        '''
        super().__init__(
            inputs=[inputs],
            outputs=[outputs],
            input_count=1,
            output_count=1
        )
        self.__resolution = resolution
        self.__number_res = self._timedelta_to_number(resolution)
        self.__datetime_key = datetime_key
        self.__volume_key = volume_key

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
        self.__buffer = None
        self.__cur_high = float("-inf")
        self.__cur_low = float("inf")
        self.__cur_open = 0
        self.__cur_close = 0
        self.__cur_volume = 0

    def _on_data(self, data: Any, index: int):
        '''
        Waits until all atom of the target resolution have been processed then outputs the result atom.
        '''
        # Skip first iteration
        if self.__buffer is None:
            self.__buffer = data
            self.__cur_open = float(data['open'])
            self.__cur_low = float(data['low'])
            self.__cur_high = float(data['high'])
            # Find the remainder of the division of time/resolution
            data_number_dt = self._datetime_to_number(th.str_to_datetime(data[self.__datetime_key]))
            remainder = data_number_dt % self.__number_res
            # Use the remainder to find the closest lower value of the resolution eg 08:15 res 1h = 08:15 - 00:15 = 08:00
            self.__cur_dt = self._number_to_datetime(data_number_dt - remainder)
            return
        # Check if the atom passed the resolution timespan
        next_dt = self.__cur_dt + self.__resolution
        if data[self.__datetime_key] < th.datetime_to_str(next_dt):
            # See if high or low are changed
            if float(data['low']) < self.__cur_low:
                self.__cur_low = float(data['low'])
            if float(data['high']) > self.__cur_high:
                self.__cur_high = float(data['high'])
            if self.__volume_key is not None:
                self.__cur_volume += int(data[self.__volume_key])
        else:
            # Time to output some data
            grouped_atom = {
                'open': str(self.__cur_open),
                'close': self.__buffer['close'],
                'high': str(self.__cur_high),
                'low': str(self.__cur_low),
                self.__datetime_key: th.datetime_to_str(self.__cur_dt)
            }
            if(self.__volume_key is not None):
                grouped_atom[self.__volume_key] = self.__cur_volume
            self._push_data(data=grouped_atom)
            # Update and reset values
            # Find the remainder of the division of time/resolution
            data_number_dt = self._datetime_to_number(th.str_to_datetime(data[self.__datetime_key]))
            remainder = data_number_dt % self.__number_res
            # Use the remainder to find the closest lower value of the resolution eg 08:15 res 1h = 08:15 - 00:15 = 08:00
            self.__cur_dt = self._number_to_datetime(data_number_dt - remainder)
            # Update values
            self.__cur_open = float(data['open'])
            self.__cur_low = float(data['low'])
            self.__cur_high = float(data['high'])
            self.__cur_volume = 0
        # Update buffer
        self.__buffer = data

    def _on_inputs_closed(self):
        '''
        All of the inputs are closed and no more data is available.
        The filter should empty itself and close all of the output streams.
        '''
        if self.__buffer is not None:
            grouped_atom = {
                'open': str(self.__cur_open),
                'close': self.__buffer['close'],
                'high': str(self.__cur_high),
                'low': str(self.__cur_low),
                self.__datetime_key: th.datetime_to_str(self.__cur_dt)
            }
            if(self.__volume_key is not None):
                grouped_atom[self.__volume_key] = self.__cur_volume
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
        return int("{:02d}{:02d}{:02d}{:02d}000".format(td.days, int(td.seconds//3600), int((td.seconds/60) % 60), int(td.seconds % 60)))
