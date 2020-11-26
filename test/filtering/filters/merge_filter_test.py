from otri.filtering.filters.merge_filter import SequentialMergeFilter
from otri.filtering.queue import LocalQueue
import unittest


class SequentialMergeFilterTest(unittest.TestCase):

    def setUp(self):
        self.inputs = [LocalQueue([1, 2, 3, 4], closed=True), LocalQueue([4, 5, 6, 7], closed=True)]
        self.outputs = [LocalQueue()]
        self.f = SequentialMergeFilter(
            inputs=["A", "B"],
            outputs="C"
        )
        self.f.setup(self.inputs, self.outputs, None)

    def test_single_execute(self):
        self.f.execute()
        self.assertEqual([LocalQueue([2, 3, 4], closed=True), LocalQueue([4, 5, 6, 7], closed=True)], self.inputs)

    def test_double_execute(self):
        # Ensures that it clears data from the first input first
        self.f.execute()
        self.f.execute()
        self.assertEqual([LocalQueue([3, 4], closed=True), LocalQueue([4, 5, 6, 7], closed=True)], self.inputs)

    def test_execute_order_async(self):
        # Ensures that it clears data from the first input first
        in_queues = [LocalQueue([1], closed=False), LocalQueue([2], closed=True)]
        m_filter = SequentialMergeFilter(
            inputs=["A", "B"],
            outputs="C"
        )
        m_filter.setup(in_queues, self.outputs, None)
        m_filter.execute()
        m_filter.execute()
        in_queues[0].append(3)
        m_filter.execute()
        self.assertEqual(self.outputs[0], LocalQueue([1, 2, 3]))

    def test_execute_order_async_2(self):
        # Ensures that it clears data from the first input first
        in_queues = [LocalQueue([1], closed=False), LocalQueue([2], closed=True)]
        m_filter = SequentialMergeFilter(
            inputs=["A", "B"],
            outputs="C"
        )
        m_filter.setup(in_queues, self.outputs, None)
        m_filter.execute()
        in_queues[0].append(3)
        m_filter.execute()
        m_filter.execute()
        self.assertEqual(LocalQueue(elements=[1, 3, 2]), self.outputs[0])

    def test_output_closed(self):
        in_queues = [LocalQueue([1], closed=False), LocalQueue([2], closed=True)]
        m_filter = SequentialMergeFilter(
            inputs=["A", "B"],
            outputs="C"
        )
        m_filter.setup(in_queues, self.outputs, None)
        m_filter.execute()
        m_filter.execute()
        in_queues[0].close()
        m_filter.execute()
        self.assertEqual(self.outputs[0], LocalQueue([1, 2]))
