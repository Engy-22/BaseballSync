from utilities.database.wrappers.baseball_data_connection import DatabaseConnection
from utilities.logger import Logger
import datetime
import os
from utilities.properties import sandbox_mode, log_prefix, import_driver_logger as driver_logger

logger = Logger(os.path.join(log_prefix, "import_data", "league_table_constructor.log"))


def league_table_constructor():
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
    db = DatabaseConnection(sandbox_mode)
    for league_id, league_name in leagues.items():
        db.write('insert into leagues (leagueId, leagueName) values ("' + league_id + '", "' + league_name + '");')
    db.close()
    logger.log('Populating leagues table completed\n\n')
    driver_logger.log('\t\tPopulating leagues table completed')


# league_table_constructor()
