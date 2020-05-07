from ..filter import Filter, Iterable

class OriginFilter(Filter):

    def __init__(self, input_streams : Iterable[Iterable]):
        super().__init__(input_streams)
        output_stream_0 = Stream()
        self.output_streams = [output_stream_0]

    def execute(self):
        self.input_streams[0]