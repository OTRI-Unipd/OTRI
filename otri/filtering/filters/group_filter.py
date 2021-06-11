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
            self.__cur_dt = self._round_time(dt=th.str_to_datetime(
                data[self.__datetime_key]), date_delta=self.__resolution, to='down')
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

    def _round_time(self, dt=datetime.now(), date_delta=timedelta(minutes=1), to='down') -> datetime:
        '''
        Rounds a datetime object to a multiple of a timedelta.

        Parameters:
            dt : datetime.datetime 
                DateTime to round, default now.
            dateDelta : timedelta
                Round to a multiple of this, default 1 minute.

        from:  http://stackoverflow.com/questions/3463930/how-to-round-the-minute-of-a-datetime-object
        '''
        round_to = date_delta.total_seconds()
        seconds = (dt.replace(tzinfo=None) - dt.min).seconds

        if seconds % round_to == 0 and dt.microsecond == 0:
            rounding = (seconds + round_to / 2) // round_to * round_to
        else:
            if to == 'up':
                # // is a floor division, not a comment on following line (like in javascript):
                rounding = (seconds + dt.microsecond/1000000 + round_to) // round_to * round_to
            elif to == 'down':
                rounding = seconds // round_to * round_to
            else:
                rounding = (seconds + round_to / 2) // round_to * round_to

        return dt + timedelta(0, rounding - seconds, - dt.microsecond)


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
        super().__init__(inputs=inputs, outputs=outputs, target_resolution=target_resolution,
                         datetime_key=datetime_key, group_handler=handler)
