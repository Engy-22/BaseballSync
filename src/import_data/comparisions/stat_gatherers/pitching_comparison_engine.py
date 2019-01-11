from import_data.comparisions.getters.pitching_getters import get_year_totals, get_pitcher_stats
from import_data.comparisions.file_writers.write_to_file_players import write_to_file
from utilities.DB_Connect import DB_Connect


def make_pitcher_comparisons(pitchers, year, possible_comps, driver_logger):
    comparisons = {}
    db, cursor = DB_Connect.grab("baseballData")
    for pitcher in pitchers:
        certainty = DB_Connect.read(cursor, 'select player_pitching.certainty from player_pitching, player_teams where '
                                            'player_pitching.PT_uniqueidentifier = player_teams.PT_uniqueidentifier and'
                                            ' player_pitching.year = ' + str(year) + ' and player_teams.playerid = "'
                                            + pitcher + '";')[0][0]
        if certainty is not None:
            if float(certainty) < 1.0:
                comparisons[pitcher] = determine_comp(pitcher, year, possible_comps, driver_logger)
    DB_Connect.close(db)
    write_to_file(year, comparisons, "pitching")


def determine_comp(pitcher, year, pitchers_to_compare, driver_logger):
    comp = None
    year_pa, year_totals = get_year_totals(year, driver_logger)
    pitcher_pa, stats = get_pitcher_stats(pitcher, year, driver_logger)
    pitcher_drs = pitcher_dr_calc(pitcher_pa, stats, year_pa, year_totals, driver_logger)
    if len(pitcher_drs) > 0:
        comp = find_comp(pitcher, year, pitcher_drs, pitchers_to_compare, driver_logger)
    return comp


def pitcher_dr_calc(pa, stats, year_pa, year_stats, driver_logger):
    pitcher_drs = {}
    for key, value in stats.items():
        if value != -1:
            try:
                dr_stat = (value / pa) / (year_stats[key] / year_pa)
                if dr_stat > 0.0:
                    pitcher_drs[key] = dr_stat
            except KeyError:
                continue
            except ZeroDivisionError:
                continue
    return pitcher_drs


def find_comp(pitcher, year, pitcher_drs, pitchers_to_compare, driver_logger):
    comp_player_scores = {}
    for year_to_compare, possible_comps in pitchers_to_compare.items():
        for possible_comp_pitcher, possible_comp_stats in possible_comps.items():
            if possible_comp_pitcher != pitcher + ';' + str(year):
                if len(possible_comp_stats) == 8:
                    comp_player_scores[possible_comp_pitcher] = 0
                    for statistic, value in possible_comp_stats.items():
                        try:
                            comp_player_scores[possible_comp_pitcher] += abs(pitcher_drs[statistic] - value)
                        except KeyError:
                            continue
    return sorted(comp_player_scores.items(), key=lambda kv: kv[1])[0][0]


# print(find_comp('lindofr01', 2018))
# print(make_pitcher_comparisons(['lindofr01', 'abreujo02', 'ramirjo01', 'pearcst01'], 2018))
