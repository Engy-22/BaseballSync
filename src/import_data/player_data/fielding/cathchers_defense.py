import time
from utilities.time_converter import time_converter
from urllib.request import urlopen
from bs4 import BeautifulSoup


def catcher_defense(year, logger):
    logger.log('\tDownloading catcher data')
    start_time = time.time()
    page = str(BeautifulSoup(urlopen('https://www.baseball-reference.com/leagues/MLB/' + str(year)
                                     + '-specialpos_c-fielding.shtml'), 'html.parser')).\
        split('Player Fielding - C</h2>')[1].split('<tbody>')[1].split('</tbody>')[0].split('<tr ')
    data = parse_table(page)
    logger.log('\t\tTime = ' + time_converter(time.time()-start_time))
    return data


def parse_table(rows):
    catchers = {}
    stats_needed = ['SB', 'CS', 'WP', 'PB']
    for row in rows:
        try:
            if 'class="thead"' not in row and 'class="league_average_table"' not in row:
                catcher = row.split('data-append-csv="')[1].split('"')[0]
                catchers[catcher] = {}
                for stat in stats_needed:
                    catchers[catcher][stat] = row.split('data-stat="' + stat + '" >')[1].split('<')[0]
        except IndexError:
            continue
    return catchers


# print(catcher_defense(2018))
