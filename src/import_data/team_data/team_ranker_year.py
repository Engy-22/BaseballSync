import time
import datetime
from utilities.DB_Connect import DB_Connect
from utilities.translate_team_id import translate_team_id
from utilities.time_converter import time_converter
from utilities.Logger import Logger

logger = Logger("C:\\Users\\Anthony Raimondo\\PycharmProjects\\baseball-sync\\logs\\import_data\\team_ranker_year.log")


def team_ranker_year(year):
    start_time = time.time()
    logger.log("Beginning year team ranker || Timestamp: " + datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S'))
    runs_for = {}
    runs_against = {}
    run_difference = {}
    teams = get_teams(year)
    for team in teams:
        games = get_games(team[0], year)
        runs_for[team[0]] = get_runs_for(team[0], year) / games
        runs_against[team[0]] = get_runs_against(team[0], year) / games
        run_difference[team[0]] = runs_for[team[0]] - runs_against[team[0]]
    scored = sorted(runs_for.items(), key=lambda kv: kv[1], reverse=True)
    allowed = sorted(runs_against.items(), key=lambda kv: kv[1])
    difference = sorted(run_difference.items(), key=lambda kv: kv[1], reverse=True)
    write_to_file(scored, year, "offRank")
    write_to_file(allowed, year, "defRank")
    write_to_file(difference, year, "ovrRank")
    logger.log("Year team ranker complete: time = " + time_converter(time.time() - start_time) + '\n\n')
    return scored, allowed, difference


def get_teams(year):
    db, cursor = DB_Connect.grab("baseballData")
    teams = DB_Connect.read(cursor, 'select teamid from team_years where year = ' + str(year) + ';')
    DB_Connect.close(db)
    return teams


def get_games(team, year):
    db, cursor = DB_Connect.grab("baseballData")
    games = int(DB_Connect.read(cursor, 'select g from team_years where teamid = "' + team + '" and year = '
                                        + str(year) + ';')[0][0])
    DB_Connect.close(db)
    return games


def get_runs_for(team, year):
    db, cursor = DB_Connect.grab("baseballData")
    runs_for = int(DB_Connect.read(cursor, 'select r from team_years where teamid = "' + team + '" and year = '
                                           + str(year) + ';')[0][0])
    DB_Connect.close(db)
    return runs_for


def get_runs_against(team, year):
    global runs_against
    db, cursor = DB_Connect.grab("baseballData")
    runs_against = int(DB_Connect.read(cursor, 'select ra from team_years where teamid = "' + team + '" and year = '
                                               + str(year) + ';')[0][0])
    DB_Connect.close(db)
    return runs_against


def write_to_file(stats, year, stat_type):
    logger.log("\tWriting " + stat_type + " data")
    start_time = time.time()
    db, cursor = DB_Connect.grab("baseballData")
    counter = 0
    for team in stats:
        team_id = translate_team_id(team[0], year)
        if DB_Connect.read(cursor, 'select league from team_years where teamId = "' + team_id + '" and year = '
                                   + str(year) + ';')[0][0].upper() in ['AL', 'NL']:
            counter += 1
            DB_Connect.write(db, cursor, 'update team_years set ' + stat_type + '_year = ' + str(counter)
                                         + ' where teamId = "' + team_id + '" and year = ' + str(year) + ';')
        else:
            continue
    DB_Connect.close(db)
    logger.log("\t\tTime = " + time_converter(time.time() - start_time))


# team_ranker_year(2018, Logger("C:\\Users\\Anthony Raimondo\\PycharmProjects\\baseball-sync\\logs\\import_data\\"
#                               "dump.log"))
