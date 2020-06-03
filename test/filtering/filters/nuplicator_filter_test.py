import unittest
from otri.filtering.filters.nuplicator_filter import NUplicatorFilter
from otri.filtering.stream import Stream


class NUplicatorFilterTest(unittest.TestCase):

    def setUp(self):
        self.source_stream = Stream()
        self.outputs = [Stream() for _ in range(3)]
        self.nuplicator = NUplicatorFilter(
            input="in",
            output=["out1", "out2", "out3"],
            deep_copy=False
        )
        self.nuplicator.setup([self.source_stream], self.outputs, None)

    def test_exactly_n_outputs(self):
        self.assertEqual(len(self.nuplicator), 3)

    def test_empty_stream(self):
        # Testing a single execute call on an empty input Stream closes the output as well
        self.source_stream.close()
        self.nuplicator.execute()
        for output in self.outputs:
            self.assertTrue(output.is_closed())

    def test_call_after_closing(self):
        # Testing a single execute call on an empty input Stream closes the output as well
        self.source_stream.close()
        self.nuplicator.execute()
        # execute again, no error should arise
        self.nuplicator.execute()
        for output in self.outputs:
            self.assertTrue(output.is_closed())

    def test_simple_stream_copying(self):
        source_stream = Stream(range(100))
        expected = source_stream[0]
        self.nuplicator.setup([source_stream], self.outputs, None)
        self.nuplicator.execute()
        for output in self.outputs:
            self.assertEqual(output[0], expected)

    def test_simple_stream_shallow(self):
        source_stream = Stream([[["Moshi Moshi"], ["Kawaii Desu"]]])
        expected = source_stream[0]
        nuplicator = NUplicatorFilter(source_stream, 5, deep_copy=False)
        nuplicator.execute()
        # Changing the inner list, change should be reflected cause copy should be shallow
        expected[0].append("Hello")
        for output in nuplicator.get_output_streams():
            self.assertEqual(output[0], expected)

    def test_simple_stream_deep(self):
        source_stream = Stream([[["Moshi Moshi"], ["Kawaii Desu"]]])
        expected = source_stream[0]
        nuplicator = NUplicatorFilter(source_stream, 5, deep_copy=True)
        nuplicator.execute()
        # Changing the inner list, change should not be reflected cause copy should be deep
        expected[0].append("Hello")
        for output in nuplicator.get_output_streams():
            self.assertNotEqual(output[0], expected)
