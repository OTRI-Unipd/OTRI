import unittest
from otri.filtering.filters.nuplicator_filter import NUplicatorFilter
from otri.filtering.stream import LocalStream


class NUplicatorFilterTest(unittest.TestCase):

    def setUp(self):
        self.source_stream = LocalStream()
        self.outputs = [LocalStream() for _ in range(3)]
        self.nuplicator = NUplicatorFilter(
            inputs="in",
            outputs=["out1", "out2", "out3"],
            deep_copy=False
        )
        self.nuplicator.setup([self.source_stream], self.outputs, None)

    def test_simple_stream_copying(self):
        source_stream = LocalStream(range(100))
        expected = 0
        self.nuplicator.setup([source_stream], self.outputs, None)
        self.nuplicator.execute()
        for output in self.outputs:
            self.assertEqual(output.read(), expected)

    def test_simple_stream_shallow(self):
        source_stream = [[["Moshi Moshi"], ["Kawaii Desu"]]]
        expected = source_stream[0]
        self.nuplicator.setup([LocalStream(source_stream)], self.outputs, None)
        self.nuplicator.execute()
        # Changing the inner list, change should be reflected cause copy should be shallow
        expected[0].append("Hello")
        for output in self.outputs:
            self.assertEqual(output.read(), expected)

    def test_simple_stream_deep(self):
        source_stream = LocalStream([[["Moshi Moshi"], ["Kawaii Desu"]]])
        expected = [["Moshi Moshi"], ["Kawaii Desu"]]
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
            self.assertNotEqual(output, LocalStream(expected))
