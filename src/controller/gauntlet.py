import random


def pick_from_options(data_dictionary):
    dart = random.random()
    board = 0
    for data_type, frequency in data_dictionary.items():
        board += frequency
        if dart < board:
            return data_type


def pick_one_or_the_other(probability, choices):
    return choices[random.random() <= probability]
