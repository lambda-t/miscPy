import sys
import json
from collections import Iterable

from util import check_if_numbers_are_consecutive
import six
import pandas as pd
import numpy as np


def _construct_key(previous_key, separator, new_key):
    """
    Returns the new_key if no previous key exists, otherwise concatenates
    previous key, separator, and new_key
    :param previous_key:
    :param separator:
    :param new_key:
    :return: a string if previous_key exists and simply passes through the
    new_key otherwise
    """
    if previous_key:
        return u"{}{}{}".format(previous_key, separator, new_key)

    else:
        return new_key


def flatten(nested_dict,key,review_id,switch="NO", separator="_", root_keys_to_ignore=set()):
    #print(nested_dict)
    """
    Flattens a dictionary with nested structure to a dictionary with no
    hierarchy
    Consider ignoring keys that you are not interested in to prevent
    unnecessary processing
    This is specially true for very deep objects
    :param nested_dict: dictionary we want to flatten
    :param separator: string to separate dictionary keys by
    :param root_keys_to_ignore: set of root keys to ignore from flattening
    :return: flattened dictionary
    """
    #assert isinstance(nested_dict, dict), "flatten requires a dictionary input"
    assert isinstance(separator, six.string_types), "separator must be string"

    # This global dictionary stores the flattened keys and values and is
    # ultimately returned
    flattened_dict = dict()
    df = pd.DataFrame()


    def _flatten(object_, key,df,switch):
        #l =[]
        #print(object_)


        """
        For dict, list and set objects_ calls itself on the elements and for
        other types assigns the object_ to
        the corresponding key in the global flattened_dict
        :param object_: object to flatten
        :param key: carries the concatenated key for the object_
        :return: None
        """
        # Empty object can't be iterated, take as is
        if not object_:
            #flattened_dict[key] = object_
            #print(key)
            flattened_dict['attribute'] = key
            flattened_dict["value"] = np.NAN
            flattened_dict["review_id"] = review_id
            #print("l",flattened_dict)
            #k.append(flattened_dict)
            #print(k)

            df2 = pd.DataFrame([flattened_dict])
            df = pd.concat([df,df2],axis=0)
            #print(df)

        # These object types support iteration
        elif isinstance(object_, dict) and switch == "NO":
            for object_key in object_:
                if not (not key and object_key in root_keys_to_ignore):
                    df =_flatten(object_[object_key], _construct_key(key,
                                                                 separator,
                                                                 object_key),df,switch)
        elif isinstance(object_, dict) and switch == "YES":
            for object_key in object_:
                #print(object_key)
                flattened_dict['attribute'] = key
                flattened_dict["value"] = object_key
                flattened_dict["review_id"] = review_id
                #print("l",flattened_dict)
                #k.append(flattened_dict)
                #print(k)

                df2 = pd.DataFrame([flattened_dict])
                df = pd.concat([df,df2],axis=0)
                #print(df)
        #Concat list elements to Key
        elif isinstance(object_, list) or isinstance(object_, set):
            #print(key,object_)
            #k.append(key)
            #print(k)
            #v.append(object_)
            #print(v)
            for e in object_:
                flattened_dict['attribute'] = key
                flattened_dict["value"] = e
                flattened_dict["review_id"] = review_id
                #print("l",flattened_dict)
                #k.append(flattened_dict)
                #print(k)

                df2 = pd.DataFrame([flattened_dict])
                df = pd.concat([df,df2],axis=0)
                #print(df)
            #for index, item in enumerate(object_):
                #print(index,item)
                #_flatten(item, _construct_key(key, "", ""))

        # Anything left take as is
        else:
            #print(key,object_)
            #If key is sentiment_label then keep value as it is
            #if "sentiment_label" in key:
            #v.append(object_)
        #flattened_dict[key] = v
            #flattened_dict[key] = object_
            #k.append(key)
            #print(k)
            #v.append(object_)
            #print(v)
            flattened_dict["attribute"] = key
            flattened_dict["value"] = object_
            flattened_dict["review_id"] = review_id
            #print("s",flattened_dict)
            #l.append(flattened_dict)
            #print(l)

            df2 = pd.DataFrame([flattened_dict])
            df = pd.concat([df,df2],axis=0)
            #print(df)
            #If key is not sentiment_label then replace value as "yes"
            #else:
                #flattened_dict[key] = "yes"
        return df
    #print df


    a = _flatten(nested_dict, key,df,switch)
    return a.reset_index()
    #print(a.reset_index())
    #print(flattened_dict)
    #print(flattened_dict)
    #return flattened_dict
    #df2 = pd.DataFrame(k)
    #df = pd.concat([df,df2],axis=0)
    #print(df)
    #print(a)

    #return flattened_dict

nested_dict = {"themes": {
    "use": {
        "theme_exists": 1,
        "sentiment_label": "pos",
        "aspects": [
            "occasion"
        ]
    },
    "fit": {
        "theme_exists": 1,
        "sentiment_label": "pos",
        "aspects": [
            "comfort",
            "test"
        ]
    }
}}
nested_dict2 = {"made_for": {

}}

#key = None
#key2 = None
#df = flatten(nested_dict2,key,"YES")
#df = df.drop(["index"],axis=1)
#print(df)


flatten_json = flatten


def _unflatten_asserts(flat_dict, separator):
    assert isinstance(flat_dict, dict), "un_flatten requires dictionary input"
    assert isinstance(separator, six.string_types), "separator must be string"
    assert all((not value or not isinstance(value, Iterable) or
                isinstance(value, six.string_types)
                for value in flat_dict.values())), "provided dict is not flat"


def unflatten(flat_dict, separator='_'):
    """
    Creates a hierarchical dictionary from a flattened dictionary
    Assumes no lists are present
    :param flat_dict: a dictionary with no hierarchy
    :param separator: a string that separates keys
    :return: a dictionary with hierarchy
    """
    _unflatten_asserts(flat_dict, separator)

    # This global dictionary is mutated and returned
    unflattened_dict = dict()

    def _unflatten(dic, keys, value):
        for key in keys[:-1]:
            dic = dic.setdefault(key, {})

        dic[keys[-1]] = value

    for item in flat_dict:
        _unflatten(unflattened_dict, item.split(separator), flat_dict[item])

    return unflattened_dict


def unflatten_list(flat_dict, separator='_'):
    """
    Unflattens a dictionary, first assuming no lists exist and then tries to
    identify lists and replaces them
    This is probably not very efficient and has not been tested extensively
    Feel free to add test cases or rewrite the logic
    Issues that stand out to me:
    - Sorting all the keys in the dictionary, which specially for the root
    dictionary can be a lot of keys
    - Checking that numbers are consecutive is O(N) in number of keys
    :param flat_dict: dictionary with no hierarchy
    :param separator: a string that separates keys
    :return: a dictionary with hierarchy
    """
    _unflatten_asserts(flat_dict, separator)

    # First unflatten the dictionary assuming no lists exist
    unflattened_dict = unflatten(flat_dict, separator)

    def _convert_dict_to_list(object_, parent_object, parent_object_key):
        if isinstance(object_, dict):
            try:
                keys = [int(key) for key in object_]
                keys.sort()
            except (ValueError, TypeError):
                keys = []
            keys_len = len(keys)

            if (keys_len > 0 and sum(keys) ==
                int(((keys_len - 1) * keys_len) / 2) and keys[0] == 0 and
                        keys[-1] == keys_len - 1 and
                    check_if_numbers_are_consecutive(keys)):

                # The dictionary looks like a list so we're going to replace it
                parent_object[parent_object_key] = []
                for key_index, key in enumerate(keys):
                    parent_object[parent_object_key].append(object_[str(key)])
                    # The list item we just added might be a list itself
                    # https://github.com/amirziai/flatten/issues/15
                    _convert_dict_to_list(parent_object[parent_object_key][-1],
                                          parent_object[parent_object_key],
                                          key_index)

            for key in object_:
                if isinstance(object_[key], dict):
                    _convert_dict_to_list(object_[key], object_, key)

    _convert_dict_to_list(unflattened_dict, None, None)
    return unflattened_dict


def cli(input_stream=sys.stdin, output_stream=sys.stdout):
    raw = input_stream.read()
    input_json = json.loads(raw)
    output = json.dumps(flatten(input_json))
    output_stream.write('{}\n'.format(output))
    output_stream.flush()


if __name__ == '__main__':
    cli()