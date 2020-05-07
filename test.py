from otri.filtering.filter_list import FilterList, FilterLayer, Filter
from otri.filtering.stream import Stream

def on_atom(atom):
    print("on_atom: {}".format(atom))

if __name__ == "__main__":
    source_stream_1 = Stream([1,2,3,4,5,6,7])
    source_stream_2 = Stream([-1,-2,-3,-4,-5,-6])
    f_list = FilterList([])
    f_list.execute([source_stream_1,source_stream_2],on_atom)
