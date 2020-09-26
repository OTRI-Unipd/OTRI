from otri.filtering.filter_net import Stream, FilterNet, FilterLayer, EXEC_AND_PASS
from otri.filtering.filters.generic_filter import GenericFilter
import unittest

EX_DATA = [1,2,3,4,5]

class FilterNetTest(unittest.TestCase):

    def setUp(self):
        self.fl = FilterNet([
            FilterLayer([
                GenericFilter(
                    inputs="A",
                    outputs="B",
                    operation=lambda x: x+1
                )
            ], EXEC_AND_PASS)
        ])
        self.input = Stream(EX_DATA, closed=True)

    def test_add_layer_works(self):
        self.fl.add_layer(FilterLayer([
            GenericFilter(
                inputs="C",
                outputs="D",
                operation=lambda x: x-1
            )
        ], EXEC_AND_PASS))

    def test_normal_execution(self):
        self.fl.execute({"A":self.input})
        self.assertEqual(self.fl.streams()['B'],[2,3,4,5,6])