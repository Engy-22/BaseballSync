import math


def time_converter(seconds):
    time = ''
    minutes = math.floor(seconds/60)
    hours = math.floor(minutes/60)
    days = math.floor(hours/24)
    if days >= 1:
        time += str(days) + ' day'
        if days == 1:
            time += ' ' + time_converter(seconds - (days * (60 * 60 * 24)))
        else:
            time += 's ' + time_converter(seconds - (days * (60 * 60 * 24)))
    elif hours >= 1:
        time += str(hours) + ' hour'
        if hours == 1:
            time += ' ' + time_converter(seconds - (hours * (60 * 60)))
        else:
            time += 's ' + time_converter(seconds - (hours * (60 * 60)))
    elif minutes >= 1:
        time += str(minutes) + ' minute'
        if minutes == 1:
            time += ' ' + time_converter(seconds - (minutes * 60))
        else:
            time += 's ' + time_converter(seconds - (minutes * 60))
    elif seconds >= 1:
        time += str(round(seconds, 2))
        if seconds == 1:
            time += ' second'
        else:
            time += ' seconds'
    return time
