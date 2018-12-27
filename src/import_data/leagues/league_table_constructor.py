from utilities.DB_Connect import DB_Connect
from utilities.Logger import Logger
import datetime

driver_logger = Logger("C:\\Users\\Anthony Raimondo\\PycharmProjects\\baseball-sync\\logs\\import_data\\driver.log")
logger = Logger("C:\\Users\\Anthony Raimondo\\PycharmProjects\\baseball-sync\\logs\\import_data\\"
                "league_table_constructor.log")


def league_table_constructor():
    driver_logger.log('Populating leagues table (all-time)')
    print('Populating leagues table (all-time)')
    logger.log('Begin populating leagues table || Timestamp: ' + datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S'))
    leagues = {'NL': 'National League',
               'AL': 'American League',
               'AA': 'American Association',
               'FL': 'Federal League',
               'PL': 'Players League',
               'UA': 'Union Association',
               'NA': 'National Association'}
    db, cursor = DB_Connect.grab("baseballData")
    for league_id, league_name in leagues.items():
        DB_Connect.write(db, cursor, 'insert into leagues (leagueId, leagueName) values ("' + league_id + '", "'
                                     + league_name + '");')
    DB_Connect.close(db)
    logger.log('Populating leagues table completed\n\n')
    driver_logger.log('\tPopulating leagues table completed')


# league_table_constructor()
