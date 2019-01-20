import time
import datetime
from utilities.logger import Logger
from import_data.comparisions.stat_gatherers.gather_players import gather_players
from import_data.comparisions.stat_gatherers.gather_teams import gather_teams
from import_data.comparisions.stat_gatherers.batting_comparison_engine import make_hitter_comparisons, hitter_dr_calc
from import_data.comparisions.getters.hitting_getters import get_year_totals as hitter_year_totals, get_hitter_stats
from import_data.comparisions.stat_gatherers.offensive_comparison_engine import make_offensive_comparisons, offensive_dr_calc
from import_data.comparisions.getters.offensive_team_getters import get_offensive_stats, get_year_totals as offensive_year_totals
from import_data.comparisions.getters.pitching_getters import get_year_totals as pitcher_year_totals, get_pitcher_stats
from import_data.comparisions.stat_gatherers.pitching_comparison_engine import make_pitcher_comparisons, pitcher_dr_calc
from import_data.comparisions.stat_gatherers.defensive_comparison_engine import make_defensive_comparisons, defensive_dr_calc
from import_data.comparisions.getters.defensive_team_getters import get_defensive_stats, get_year_totals as defensive_year_totals
from utilities.time_converter import time_converter

logger = Logger("C:\\Users\\Anthony Raimondo\\PycharmProjects\\baseball-sync\\logs\\import_data\\"
                "comparisons_driver.log")


def comparisons_driver(most_recent_year, driver_logger):
    driver_logger.log("\tBeginning comparisons driver")
    start_time = time.time()
    logger.log("Beginning comparisons driver || Timestamp: " + datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S'))
    possible_hitter_comps = {}
    possible_pitcher_comps = {}
    possible_offensive_comps = {}
    possible_defensive_comps = {}

    print('making hitter comparisons (overall)')
    driver_logger.log('\t\tMaking hitter comparisons (overall)')
    hc_time = time.time()
    logger.log("\tMaking hitter comparisons (overall)")
    logger.log('\t\tGathering list of possible comps')
    for year_to_compare in [year for year in range(most_recent_year, 1997, -1)]:
        possible_hitter_comps[year_to_compare] = {}
        year_pa, year_totals = hitter_year_totals(year_to_compare, logger)
        for comp_hitter in gather_players(year_to_compare, "batting", True, logger):
            comp_hitter_pa, comp_stats = get_hitter_stats(comp_hitter, year_to_compare)
            if comp_hitter_pa >= 300:
                possible_hitter_comps[year_to_compare][comp_hitter + ';' + str(year_to_compare)] =\
                    hitter_dr_calc(comp_hitter_pa, comp_stats, year_pa, year_totals)
    logger.log('\t\tBegin calculating comparisons scores')
    for comp_year in range(1876, most_recent_year+1, 1):
        try:
            year_pa, year_totals = hitter_year_totals(comp_year, logger)
        except IndexError:
            continue
        make_hitter_comparisons(gather_players(comp_year, "batting", False, logger), comp_year, possible_hitter_comps,
                                year_pa, year_totals, logger)
    total_time = time_converter(time.time() - hc_time)
    logger.log("\t\tTime = " + total_time)
    driver_logger.log('\t\t\tTime = ' + total_time)

    print('making pitcher comparisons (overall)')
    driver_logger.log('\t\tMaking pitcher comparisons (overall)')
    pc_time = time.time()
    logger.log("\tMaking pitcher comparisons (overall)")
    logger.log('\t\tGathering list of possible comps')
    for year_to_compare in [year for year in range(most_recent_year, 1996, -1)]:
        possible_pitcher_comps[year_to_compare] = {}
        year_pa, year_totals = pitcher_year_totals(year_to_compare, logger)
        for comp_pitcher in gather_players(year_to_compare, "pitching", True, logger):
            comp_pitcher_pa, comp_stats = get_pitcher_stats(comp_pitcher, year_to_compare)
            if comp_pitcher_pa >= 200:
                possible_pitcher_comps[year_to_compare][comp_pitcher + ';' + str(year_to_compare)] = \
                    pitcher_dr_calc(comp_pitcher_pa, comp_stats, year_pa, year_totals)
    logger.log('\t\tBegin calculating comparisons scores')
    for comp_year in range(1876, most_recent_year+1, 1):
        try:
            year_pa, year_totals = pitcher_year_totals(comp_year, logger)
        except IndexError:
            continue
        make_pitcher_comparisons(gather_players(comp_year, "pitching", False, logger), comp_year,
                                 possible_pitcher_comps, year_pa, year_totals, logger)
    total_time = time_converter(time.time() - pc_time)
    logger.log("\t\tTime = " + total_time)
    driver_logger.log('\t\t\tTime = ' + total_time)

    print('making team offensive comparisons')
    driver_logger.log('\t\tMaking team offensive comparisons')
    logger.log('\t\tGathering list of possible comps')
    oc_time = time.time()
    logger.log("\tMaking offensive comparisons (overall)")
    for year_to_compare in [year for year in range(most_recent_year, 1997, -1)]:
        year_pa, year_totals = hitter_year_totals(year_to_compare, logger)
        for comp_ty_uid in gather_teams(year_to_compare, logger):
            comp_team_pa, comp_stats = get_offensive_stats(comp_ty_uid)
            possible_offensive_comps[comp_ty_uid] = offensive_dr_calc(comp_team_pa, comp_stats, year_pa, year_totals)
    logger.log('\t\tBegin calculating comparisons scores')
    for comp_year in range(1876, most_recent_year+1, 1):
        try:
            year_pa, year_totals = offensive_year_totals(comp_year, logger)
        except IndexError:
            continue
        make_offensive_comparisons(gather_teams(comp_year, logger), possible_offensive_comps, year_pa, year_totals,
                                   logger)
    total_time = time_converter(time.time() - oc_time)
    logger.log("\t\tTime = " + total_time)
    driver_logger.log('\t\t\tTime = ' + total_time)

    print('making team defensive comparisons')
    driver_logger.log('\t\tMaking team defensive comparisons')
    logger.log('\t\tGathering list of possible comps')
    dc_time = time.time()
    logger.log("\tMaking defensive comparisons (overall)")
    for year_to_compare in [year for year in range(most_recent_year, 1997, -1)]:
        year_pa, year_totals = defensive_year_totals(year_to_compare, logger)
        for comp_ty_uid in gather_teams(year_to_compare, logger):
            comp_team_pa, comp_stats = get_defensive_stats(comp_ty_uid)
            possible_defensive_comps[comp_ty_uid] = defensive_dr_calc(comp_team_pa, comp_stats, year_pa, year_totals)
    logger.log('\t\tBegin calculating comparisons scores')
    for comp_year in range(1876, most_recent_year+1, 1):
        try:
            year_pa, year_totals = defensive_year_totals(comp_year, logger)
        except IndexError:
            continue
        make_defensive_comparisons(gather_teams(comp_year, logger), possible_defensive_comps, year_pa, year_totals,
                                   logger)
    total_time = time_converter(time.time() - dc_time)
    logger.log("\t\tTime = " + total_time)
    driver_logger.log('\t\t\tTime = ' + total_time)

    total_time = time_converter(time.time() - start_time)
    logger.log("Done making comparisons: time = " + total_time + '\n\n')
    driver_logger.log("\tDone making comparisons: time = " + total_time)


from utilities.get_most_recent_year import get_most_recent_year
dump = Logger("C:\\Users\\Anthony Raimondo\\PycharmProjects\\baseball-sync\\logs\\import_data\\dump.log")
comparisons_driver(get_most_recent_year(dump), dump)
