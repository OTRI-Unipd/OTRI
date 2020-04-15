import json

LOWER_ERROR = "Only dictionaries and lists can be modified by this method."

def lower_all_keys_deep(data):
    '''
    Renames all the keys to be lower case.
    The method is recursive and applies as deep as possible in the json nest. 

    Parameters:
        data : dict | list
            Data to modify, must be either a dictionary or a list of dictionaries.
    Returns:
        A copy of the dict or list with the renamed keys, and where all nested dicts and list
        will receive the same treatment it will return the original
        object (not a copy) if no operation could be applied, for example when:
        - data is not a list or dict
        - data is a list of non dict items
        - data is not a list that contains dicts at any nesting level
        ...
    '''
    if type(data) == dict:
        return _lower_keys_dict(data)
    if type(data) == list:
        return _lower_keys_list(data)
    return data


def _lower_keys_dict(data: dict):
    '''
    Lowers all keys in a dictionary and all nested items.

    Parameters:
        data : dict
            Data to modify, must be a dictionary.
    Returns:
        A copy of the dict with the renamed keys, where all values have been replaced by copies of
        their original if lower_all_keys(value) was appliable.
    '''
    new_data = dict()
    for key, value in data.items():
        lower_key = key.lower()
        new_data[lower_key] = lower_all_keys_deep(value)
    return new_data


def _lower_keys_list(data: list):
    '''
    Lowers all keys in each item of the list, if appliable.

    Parameters:
        data : list
            Data to modify, should be a list, but can be a tuple.
    Returns:
        A copy of the list, where each item got its keys lowered through lower_all_keys(item) if appliable.
    '''
    return [lower_all_keys_deep(item) for item in data]

# Just some test code
if __name__ == "__main__":
    test1 = {"Test":"限界を越える"}
    print("test1: {}".format(lower_all_keys_deep(test1)))
    test2 = [{"Test":"限界を越える"}, {"Another test":"おれ を だれ だ と おもって やがる？"}]
    print("test1: {}".format(lower_all_keys_deep(test2)))
    test3 = {"Test": test2}
    print("test3: {}".format(lower_all_keys_deep(test3)))