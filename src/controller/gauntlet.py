import random
from collections import OrderedDict


def pick_from_options(data_dictionary):
    dart = random.random()
    board = 0
    for data_type, frequency in data_dictionary.items():
        board += frequency
        if dart < board:
            return data_type
    try:
        return list(OrderedDict(data_dictionary))[-1]
    except IndexError:
        return None


def pick_one_or_the_other(probability, choices):
    return choices[random.random() <= probability]
