import random


def determine(data_dictionary):
    dart = random.random()
    board = 0
    for data_type, frequency in data_dictionary.items():
        board += frequency
        if dart < board:
            return data_type
