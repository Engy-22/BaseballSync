import time
import datetime
from utilities.Logger import Logger
from import_data.comparisions.stat_gatherers.gather_players import gather_players
from import_data.comparisions.stat_gatherers.gather_teams import gather_teams
from import_data.comparisions.stat_gatherers.batting_comparison_engine import make_hitter_comparisons, hitter_dr_calc
from import_data.comparisions.getters.hitting_getters import get_year_totals as hitter_year_totals, get_hitter_stats
from import_data.comparisions.stat_gatherers.offensive_comparison_engine import make_offensive_comparisons, offensive_dr_calc
from import_data.comparisions.getters.offensive_team_getters import get_offensive_stats
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
    hitters_to_compare = {}
    possible_pitcher_comps = {}
    pitchers_to_compare = {}
    possible_offensive_comps = {}
    offenses_to_compare = {}
    possible_defensive_comps = {}
    defenses_to_compare = {}

    # print('making hitter comparisons (overall)')
    # hc_time = time.time()
    # logger.log("\tMaking hitter comparisons (overall)")
    # hitter_years_to_compare = [year for year in range(most_recent_year, 1997, -1)]
    # for year_to_compare in hitter_years_to_compare:
    #     possible_hitter_comps[year_to_compare] = {}
    #     year_pa, year_totals = hitter_year_totals(year_to_compare, logger)
    #     hitters_to_compare[year_to_compare] = gather_players(year_to_compare, "batting", logger)
    #     for comp_ty_uid in hitters_to_compare[year_to_compare]:
    #         comp_team_pa, comp_stats = get_hitter_stats(comp_ty_uid, year_to_compare, logger)
    #         if comp_team_pa >= 300:
    #             possible_hitter_comps[year_to_compare][comp_ty_uid + ';' + str(year_to_compare)] =\
    #                 hitter_dr_calc(comp_team_pa, comp_stats, year_pa, year_totals, logger)
    # for comp_year in range(1876, most_recent_year + 1, 1):
    #     make_hitter_comparisons(gather_players(comp_year, "batting", logger), comp_year, possible_hitter_comps, logger)
    # logger.log("\t\tTime = " + time_converter(time.time() - hc_time))

    print('making pitcher comparisons (overall)')
    pc_time = time.time()
    logger.log("\tMaking pitcher comparisons (overall)")
    pitcher_years_to_compare = [year for year in range(most_recent_year, 1996, -1)]
    for year_to_compare in pitcher_years_to_compare:
        possible_pitcher_comps[year_to_compare] = {}
        year_pa, year_totals = pitcher_year_totals(year_to_compare, logger)
        pitchers_to_compare[year_to_compare] = gather_players(year_to_compare, "pitching", logger)
        for comp_pitcher in pitchers_to_compare[year_to_compare]:
            comp_pitcher_pa, comp_stats = get_pitcher_stats(comp_pitcher, year_to_compare, logger)
            if comp_pitcher_pa >= 300:
                possible_pitcher_comps[year_to_compare][comp_pitcher + ';' + str(year_to_compare)] = \
                    pitcher_dr_calc(comp_pitcher_pa, comp_stats, year_pa, year_totals, logger)
    for comp_year in range(1876, most_recent_year + 1, 1):
        make_pitcher_comparisons(gather_players(comp_year, "pitching", logger), comp_year, possible_pitcher_comps, logger)
    logger.log("\t\tTime = " + time_converter(time.time() - pc_time))

    print('making team offensive comparisons')
    oc_time = time.time()
    logger.log("\tMaking offensive comparisons (overall)")
    offensive_years_to_compare = [year for year in range(most_recent_year, 1997, -1)]
    for year_to_compare in offensive_years_to_compare:
        year_pa, year_totals = hitter_year_totals(year_to_compare, logger)
        offenses_to_compare[year_to_compare] = gather_teams(year_to_compare, logger)
        for comp_ty_uid in offenses_to_compare[year_to_compare]:
            comp_team_pa, comp_stats = get_offensive_stats(comp_ty_uid, logger)
            possible_offensive_comps[comp_ty_uid] = offensive_dr_calc(comp_team_pa, comp_stats, year_pa, year_totals, logger)
    for comp_year in range(1876, most_recent_year + 1, 1):
        make_offensive_comparisons(gather_teams(comp_year, logger), comp_year, possible_offensive_comps, logger)
    logger.log("\t\tTime = " + time_converter(time.time() - oc_time))

    print('making team defensive comparisons')
    dc_time = time.time()
    logger.log("\tMaking defensive comparisons (overall)")
    defensive_years_to_compare = [year for year in range(most_recent_year, 1997, -1)]
    for year_to_compare in defensive_years_to_compare:
        year_pa, year_totals = defensive_year_totals(year_to_compare, logger)
        defenses_to_compare[year_to_compare] = gather_teams(year_to_compare, logger)
        for comp_ty_uid in defenses_to_compare[year_to_compare]:
            comp_team_pa, comp_stats = get_defensive_stats(comp_ty_uid, driver_logger)
            possible_defensive_comps[comp_ty_uid] = defensive_dr_calc(comp_team_pa, comp_stats, year_pa, year_totals, logger)
    for comp_year in range(1876, most_recent_year + 1, 1):
        make_defensive_comparisons(gather_teams(comp_year, logger), comp_year, possible_defensive_comps, logger)
    logger.log("\t\tTime = " + time_converter(time.time() - dc_time))

    total_time = time_converter(time.time() - start_time)
    logger.log("Done making comparisons: time = " + total_time + '\n\n')
    driver_logger.log("\t\tDone making comparisons: time = " + total_time)


# from utilities.get_most_recent_year import get_most_recent_year
# dump = Logger("C:\\Users\\Anthony Raimondo\\PycharmProjects\\baseball-sync\\logs\\import_data\\dump.log")
# comparisons_driver(get_most_recent_year(dump), dump)
