from utilities.connections.baseball_data_connection import DatabaseConnection
from utilities.logger import Logger
import datetime

logger = Logger("C:\\Users\\Anthony Raimondo\\PycharmProjects\\baseball-sync\\logs\\import_data\\"
                "league_table_constructor.log")


def league_table_constructor(driver_logger, sandbox_mode):
    driver_logger.log('\tPopulating leagues table (all-time)')
    print('Populating leagues table (all-time)')
    logger.log('Begin populating leagues table || Timestamp: ' + datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S'))
    leagues = {'NL': 'National League',
               'AL': 'American League',
               'AA': 'American Association',
               'FL': 'Federal League',
               'PL': 'Players League',
               'UA': 'Union Association',
               'NA': 'National Association'}
    db = DatabaseConnection()
    for league_id, league_name in leagues.items():
        db.write('insert into leagues (leagueId, leagueName) values ("' + league_id + '", "' + league_name + '");')
    db.close()
    logger.log('Populating leagues table completed\n\n')
    driver_logger.log('\t\tPopulating leagues table completed')


# league_table_constructor()
