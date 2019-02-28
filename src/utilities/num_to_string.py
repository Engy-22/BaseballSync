def num_to_string(num):
    stringed_num = ''
    i = 1
    for digit in reversed(str(num)):
        stringed_num = digit + stringed_num
        if i % 3 == 0:
            stringed_num = ',' + stringed_num
        i += 1
    if stringed_num[0] == ',':
        return stringed_num[1:]
    else:
        return stringed_num

