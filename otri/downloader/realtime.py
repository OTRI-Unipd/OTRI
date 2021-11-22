import signal

from . import Adapter


class RealtimeAdapter(Adapter):
    '''
    Adapter to continuosly download data from a provider. Runs preparation component only once.
    Handles thread signals to be stopped (sigterm, sighup and sigint) or by calling stop().
    '''

    def download(self, output, **kwargs) -> None:
        '''
        Retrieves data from a source using the components.
        Calls preparation once then continuosly downloads data until stopped.
        To stop download either raise one of the stop signals or call stop().

        Parameters:
            output
                Collection with .append() method
            Any parameter that the components use.
        Returns:
            A list of atoms, the same object as parameters o_stream.
        '''
        kwargs = self._prepare(**kwargs)

        if 'buffer' in kwargs:
            raise ValueError("buffer parameter is a reserved argument name")

        # Prepares to handle sig-term sig-hup and sig-int
        for signal_name in ('SIGTERM', 'SIGHUP', 'SIGINT'):
            signal.signal(getattr(signal, signal_name), self.stop)
        self._running = True

        data_buffer = []  # Connecting pipe for data between components

        while(self._running):
            self._retrieve(buffer=data_buffer, output=output, **kwargs)
            data_buffer.clear()  # Make sure to clear the data buffer between iterations

    def stop(self, *args, **kwargs):
        '''
        Stops the adapter.
        '''
        self._running = False
