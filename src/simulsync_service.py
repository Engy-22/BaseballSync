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
            if url not in completed_games:
                if url not in games_in_progress:
                    games_in_progress[url] = {'latest_pitch': 'get_the_latest_pitch'}
                if 'gid' in url:
                    try:
                        game_status, new_pitch, latest_pitch, pitch_description, pitch_type, pitch_outcome, x, y, \
                        velocity, batter, pitcher, outcome = fetch_game(url.split('<a href="')[0] + '/'
                                                                        + url.split('<a href="')[1].split('/')[1] + '/',
                                                                        games_in_progress, url)
                        if new_pitch:
                            print(url.split('gid')[1].split('_')[4][0:3] + ' vs. '
                                  + url.split('gid')[1].split('_')[5][0:3] + ' --> ', pitch_description, pitch_type,
                                  velocity + ' mph', batter, pitcher, outcome)
                        if game_status == 'completed':
                            completed_games.append(url)
                            del games_in_progress[url]
                        elif game_status == 'in progress':
                            games_in_progress[url]['latest_pitch'] = latest_pitch
                        else:
                            continue
                    except IndexError:
                        continue
        # print('\n')
        time.sleep(10)


driver()
