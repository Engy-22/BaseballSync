import os
import sys
from simulsync.fetch import fetch_home_pages, fetch_game

sys.path.append(os.path.join(sys.path[0], '..', '..'))


def driver():
    for url in fetch_home_pages():
        if 'gid' in url:
            try:
                print('\n' + fetch_game(url.split('<a href="')[0]+url.split('<a href="')[1].split('">')[0]))
            except IndexError:
                continue


driver()
