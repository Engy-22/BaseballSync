import os
from utilities.properties import log_prefix as parent_dir


def clear_logs(sub_directory):
    for log_file in os.listdir(os.path.join(parent_dir, sub_directory)):
        f = open(os.path.join(parent_dir, sub_directory, log_file), 'w')
        f.close()
