import os
from utilities.properties import log_prefix as parent_dir


def clear_logs():
    for sub_dir in os.listdir(parent_dir):
        for log_file in os.listdir(os.path.join(parent_dir, sub_dir)):
            f = open(os.path.join(parent_dir, sub_dir, log_file), 'w')
            f.close()
