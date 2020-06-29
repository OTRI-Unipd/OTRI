import unittest
from otri.filtering.filters.nuplicator_filter import NUplicatorFilter
from otri.filtering.stream import Stream


class NUplicatorFilterTest(unittest.TestCase):

    def setUp(self):
        self.source_stream = Stream()
        self.outputs = [Stream() for _ in range(3)]
        self.nuplicator = NUplicatorFilter(
            inputs="in",
            outputs=["out1", "out2", "out3"],
            deep_copy=False
        )
        self.nuplicator.setup([self.source_stream], self.outputs, None)

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
        self.nuplicator.setup([source_stream], self.outputs, None)
        self.nuplicator.execute()
        # Changing the inner list, change should be reflected cause copy should be shallow
        expected[0].append("Hello")
        for output in self.outputs:
            self.assertEqual(output[0], expected)

    def test_simple_stream_deep(self):
        source_stream = Stream([[["Moshi Moshi"], ["Kawaii Desu"]]])
        expected = source_stream[0]
        nuplicator = NUplicatorFilter(
            inputs="in",
            outputs=["out1", "out2", "out3"],
            deep_copy=True
        )
        nuplicator.setup([source_stream], self.outputs, None)
        nuplicator.execute()
        # Changing the inner list, change should not be reflected cause copy should be deep
        expected[0].append("Hello")
        for output in self.outputs:
            self.assertNotEqual(output[0], expected)
