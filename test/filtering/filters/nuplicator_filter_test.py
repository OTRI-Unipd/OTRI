import unittest
from otri.filtering.filters.nuplicator_filter import NUplicatorFilter
from otri.filtering.queue import LocalQueue


class NUplicatorFilterTest(unittest.TestCase):

    def setUp(self):
        self.source_queue = LocalQueue()
        self.outputs = [LocalQueue() for _ in range(3)]
        self.nuplicator = NUplicatorFilter(
            inputs="in",
            outputs=["out1", "out2", "out3"],
            deep_copy=False
        )
        self.nuplicator.setup([self.source_queue], self.outputs, None)

    def test_simple_queue_copying(self):
        source_queue = LocalQueue(range(100))
        expected = 0
        self.nuplicator.setup([source_queue], self.outputs, None)
        self.nuplicator.execute()
        for output in self.outputs:
            self.assertEqual(output.read(), expected)

    def test_simple_queue_shallow(self):
        source_queue = [[["Moshi Moshi"], ["Kawaii Desu"]]]
        expected = source_queue[0]
        self.nuplicator.setup([LocalQueue(source_queue)], self.outputs, None)
        self.nuplicator.execute()
        # Changing the inner list, change should be reflected cause copy should be shallow
        expected[0].append("Hello")
        for output in self.outputs:
            self.assertEqual(output.read(), expected)

    def test_simple_queue_deep(self):
        source_queue = LocalQueue([[["Moshi Moshi"], ["Kawaii Desu"]]])
        expected = [["Moshi Moshi"], ["Kawaii Desu"]]
        nuplicator = NUplicatorFilter(
            inputs="in",
            outputs=["out1", "out2", "out3"],
            deep_copy=True
        )
        nuplicator.setup([source_queue], self.outputs, None)
        nuplicator.execute()
        # Changing the inner list, change should not be reflected cause copy should be deep
        expected[0].append("Hello")
        for output in self.outputs:
            self.assertNotEqual(output, LocalQueue(expected))
