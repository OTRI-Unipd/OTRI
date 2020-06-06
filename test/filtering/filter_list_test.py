from otri.filtering.filter_list import Stream, FilterList, FilterLayer
from otri.filtering.filters.generic_filter import GenericFilter
import unittest

EX_DATA = [1,2,3,4,5]

class FilterListTest(unittest.TestCase):

    def setUp(self):
        self.fl = FilterList([
            GenericFilter(
                inputs="A",
                outputs="B",
                operation=lambda x: x+1
            )
        ])
        self.input = Stream(EX_DATA, is_closed=True)