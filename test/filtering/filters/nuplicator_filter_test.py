import unittest
from otri.filtering.filters.nuplicator_filter import NUplicatorFilter
from otri.filtering.stream import Stream


class NUplicatorFilterTest(unittest.TestCase):

    def test_single_input_stream(self):
        # Testing the input stream is exactly the one we gave
        source_stream = Stream()
        nuplicator = NUplicatorFilter(source_stream, 1)
        self.assertEqual(len(nuplicator.get_input_streams()), 1)
        self.assertEqual(nuplicator.get_input_streams()[0], source_stream)

    def test_exactly_n_outputs(self):
        # Testing we get exactly n outputs
        source_stream = Stream()
        expected = 10
        nuplicator = NUplicatorFilter(source_stream, expected)
        self.assertEqual(len(nuplicator.get_output_streams()), expected)

    def test_simple_stream_copying(self):
        source_stream = Stream(range(100))
        expected = source_stream[0]
        nuplicator = NUplicatorFilter(source_stream, 5)
        nuplicator.execute()
        for output in nuplicator.get_output_streams():
            self.assertFalse(output[0] != expected)

    def test_simple_stream_shallow(self):
        source_stream = Stream([[["Moshi Moshi"], ["Kawaii Desu"]]])
        expected = source_stream[0]
        nuplicator = NUplicatorFilter(source_stream, 5)
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
