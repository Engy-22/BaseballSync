def stringify_list(given_list):
    if len(given_list) > 1:
        low = str(given_list[0])
        high = str(given_list[-1])
        if len(given_list) != 2:
            return 's ' + low + '-' + high
        else:
            return 's ' + low + ' & ' + high
    else:
        return ' ' + str(given_list[0])
