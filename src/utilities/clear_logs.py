import os


def clear_logs(parent_dir):
    for sub_dir in os.listdir(parent_dir):
        for log_file in os.listdir(os.path.join(parent_dir, sub_dir)):
            f = open(os.path.join(parent_dir, sub_dir, log_file), 'w')
            f.close()


# clear_logs()
