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


def get_location(mean, standard_deviation):
    """this function gets the location (could be x or y) of a pitch based on the average mean and average standard
    deviation of the hitter and pitcher"""
    areas = {}
    for area in range(12):  # this is to create a dictionary to find where the pitch will be thrown. 12 intervals creates 3 standard deviations in each direction from the mean (0.5 units each).
        areas[area] = 1/12
    index = pick_from_options(areas)
    if index < 6:  # if the pitch is within one standard deviation of the mean
        deviation_range = [[mean - standard_deviation, mean], [mean, mean + standard_deviation]]
    elif index < 9:  # if the pitch is within two standard deviations of the mean
        deviation_range = [[mean - (2*standard_deviation), mean - standard_deviation],
                           [mean + standard_deviation, mean + (2*standard_deviation)]]
    else:  # the pitch is three standard deviations from the mean
        deviation_range = [[mean - (3*standard_deviation), mean - (2*standard_deviation)],
                           [mean + (2*standard_deviation), mean + (3*standard_deviation)]]
    deviation_range_index = random.randint(0, 1)  # decide to use lower or higher list range deviation_range
    return random.uniform(deviation_range[deviation_range_index][0], deviation_range[deviation_range_index][1])
