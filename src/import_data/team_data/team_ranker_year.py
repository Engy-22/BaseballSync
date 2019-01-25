import time
import datetime
from utilities.dbconnect import DatabaseConnection
from utilities.translate_team_id import translate_team_id
from utilities.time_converter import time_converter
from utilities.logger import Logger

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
    db = DatabaseConnection()
    teams = db.read('select teamid from team_years where year = ' + str(year) + ';')
    db.close()
    return teams


def get_games(team, year):
    db = DatabaseConnection()
    games = int(db.read('select g from team_years where teamid = "' + team + '" and year = ' + str(year) + ';')[0][0])
    db.close()
    return games


def get_runs_for(team, year):
    db = DatabaseConnection()
    runs_for = int(db.read('select r from team_years where teamid = "' + team + '" and year=' + str(year) + ';')[0][0])
    db.close()
    return runs_for


def get_runs_against(team, year):
    global runs_against
    db = DatabaseConnection()
    runs_against = int(db.read('select ra from team_years where teamid = "' + team + '" and year = ' + str(year)
                               + ';')[0][0])
    db.close()
    return runs_against


def write_to_file(stats, year, stat_type):
    logger.log("\tWriting " + stat_type + " data")
    start_time = time.time()
    db = DatabaseConnection()
    counter = 0
    for team in stats:
        team_id = translate_team_id(team[0], year)
        if db.read('select league from team_years where teamId = "' + team_id + '" and year = ' + str(year)
                   + ';')[0][0].upper() in ['AL', 'NL']:
            counter += 1
            db.write('update team_years set ' + stat_type + '_year = ' + str(counter) + ' where teamId = "' + team_id
                     + '" and year = ' + str(year) + ';')
        else:
            continue
    db.close()
    logger.log("\t\tTime = " + time_converter(time.time() - start_time))


# team_ranker_year(2018, Logger("C:\\Users\\Anthony Raimondo\\PycharmProjects\\baseball-sync\\logs\\import_data\\"
#                               "dump.log"))
