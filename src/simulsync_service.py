import os
import sys
import time
from simulsync.fetch import fetch_home_pages, fetch_game

sys.path.append(os.path.join(sys.path[0], '..', '..'))


def driver():
    completed_games = []
    games_in_progress = {}
    while True:
        for url in fetch_home_pages():
            try:
                this_game = url.split('<a href=')[1].split('_')[5:7]
                if len(this_game) > 0:
                    print(this_game)
            except IndexError:
                pass
            if url not in completed_games:
                if url not in games_in_progress:
                    games_in_progress[url] = {'latest_pitch': 'get_the_latest_pitch'}
                if 'gid' in url:
                    try:
                        game, latest_pitch = \
                            fetch_game(url.split('<a href="')[0] + '/' + url.split('<a href="')[1].split('/')[1] + '/',
                                       games_in_progress[url])
                        if game == 'completed':
                            completed_games.append(url)
                            del games_in_progress[url]
                        elif game == 'in progress':
                            games_in_progress[url]['latest_pitch'] = latest_pitch
                        else:
                            continue
                    except IndexError:
                        continue
        print('\n')
        time.sleep(10)


driver()
