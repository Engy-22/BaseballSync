import os


def clear_logs():
    parent_dir = "C:\\Users\\Anthony Raimondo\\PycharmProjects\\baseball-sync\\logs"
    for sub_dir in os.listdir(parent_dir):
        for log_file in os.listdir(parent_dir + '\\' + sub_dir):
            f = open(parent_dir + '\\' + sub_dir + '\\' + log_file, 'w')
            f.close()


# clear_logs()
