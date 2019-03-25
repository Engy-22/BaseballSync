import os
import sys
from simulsync.trigger import fetch_pages

sys.path.append(os.path.join(sys.path[0], '..'))


def driver():
    return fetch_pages()
