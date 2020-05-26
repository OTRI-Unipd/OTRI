from otri.filtering.filters.merge_filter import SequentialMergeFilter, Stream
import unittest

class SequentialMergeFilterTest(unittest.TestCase):

    def setUp(self):
        self.input_streams = [Stream([1,2,3,4],is_closed=True), Stream([4,5,6,7],is_closed=True)]
        self.default_filter = SequentialMergeFilter(self.input_streams)

    def test_single_execute(self):
        self.default_filter.execute()
        self.assertEqual([Stream([2,3,4],is_closed=True),Stream([4,5,6,7],is_closed=True)], self.default_filter.get_input_streams())

    def test_double_execute(self):
        # Ensures that it clears data from the first input first
        self.default_filter.execute()
        self.default_filter.execute()
        self.assertEqual([Stream([3,4],is_closed=True),Stream([4,5,6,7],is_closed=True)], self.default_filter.get_input_streams())

    def test_execute_order_async(self):
        # Ensures that it clears data from the first input first
        streams = [Stream([1],is_closed=False), Stream([2],is_closed=True)]
        m_filter = SequentialMergeFilter(streams)
        m_filter.execute()
        m_filter.execute()
        streams[0].append(3)
        m_filter.execute()
        self.assertEqual([1,2,3], m_filter.get_output_stream(0))

    def test_execute_order_async_2(self):
        # Ensures that it clears data from the first input first
        streams = [Stream([1],is_closed=False), Stream([2],is_closed=True)]
        m_filter = SequentialMergeFilter(streams)
        m_filter.execute()
        streams[0].append(3)
        m_filter.execute()
        m_filter.execute()
        self.assertEqual([1,3,2], m_filter.get_output_stream(0))

    def test_output_closed(self):
        streams = [Stream([1],is_closed=False), Stream([2],is_closed=True)]
        m_filter = SequentialMergeFilter(streams)
        m_filter.execute()
        m_filter.execute()
        streams[0].close()
        m_filter.execute()
        self.assertEqual([1,2], m_filter.get_output_stream(0))
        self.assertTrue(m_filter.is_finished())