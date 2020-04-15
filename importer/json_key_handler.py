import json

LOWER_ERROR = "Only dictionaries and lists can be modified by this method."

def apply_deep(data, fun):
    '''
    Applies fun to all keys in data.
    The method is recursive and applies as deep as possible in the json nest. 

    Parameters:
        data : dict | list
            Data to modify, must be either a dictionary or a list of dictionaries.
        fun : function | lambda
            Function to apply to each key, must take the key as its single parameter.
    Returns:
        A copy of the dict or list with the modified keys, and where all nested dicts and list
        will receive the same treatment it will return the original
        object (not a copy) if no operation could be applied, for example when:
        - data is not a list or dict
        - data is a list of non dict items
        - data is not a list that contains dicts at any nesting level
        ...
    '''
    if type(data) == dict:
        return _apply_deep_dict(data, fun)
    if type(data) == list:
        return _apply_deep_list(data, fun)
    return data

def _apply_deep_dict(data : dict, fun):
    '''
    Applies fun to all keys in a dictionary and all nested items.

    Parameters:
        data : dict
            Data to modify, must be a dictionary.
        fun : function | lambda
            Function to apply to each key, must take the key as its single parameter.
    Returns:
        A copy of the dict with the renamed keys, where all values have been replaced by copies of
        their original if apply_deep(value, fun) was appliable.
    '''
    new_data = dict()
    for key, value in data.items():
        new_key = fun(key)
        new_data[new_key] = apply_deep(value, fun)
    return new_data

def _apply_deep_list(data: list, fun):
    '''
    Applies fun to all keys in each item of the list, if appliable.

    Parameters:
        data : list
            Data to modify, should be a list, but can be a tuple.
        fun : function | lambda
            Function to apply to each key, must take the key as its single parameter.
    Returns:
        A copy of the list, where each item got its keys modified through apply_deep(item, fun) if appliable.
    '''
    return [apply_deep(item, fun) for item in data]

def lower_all_keys_deep(data):
    '''
    Renames all the keys in a dict object to be lower case.
    The method is recursive and applies as deep as possible in the json nest. 

    Parameters:
        data : dict | list
            Data to modify, must be either a dictionary or a list of dictionaries.
            Technically meant to work with a json object dictionary, should work with
            any dictionary. In any case, only string keys will be modified.
    Returns:
        A copy of the dict or list with the renamed keys, and where all nested dicts and list
        will receive the same treatment it will return the original
        object (not a copy) if no operation could be applied, for example when:
        - data is not a list or dict
        - data is a list of non dict items
        - data is not a list that contains dicts at any nesting level
        ...
    '''
    return apply_deep(data, lambda s : s.lower() if type(s) == str else s)

# Just some test code
if __name__ == "__main__":
    test1 = {"Test":"限界を越える"}
    print("test1: {}".format(lower_all_keys_deep(test1)))
    test2 = [{"Test":"限界を越える"}, {"Another test":"おれ を だれ だ と おもって やがる？"}]
    print("test1: {}".format(lower_all_keys_deep(test2)))
    test3 = {"Test": test2}
    print("test3: {}".format(lower_all_keys_deep(test3)))