from import_data.comparisions.getters.offensive_team_getters import get_year_totals, get_offensive_stats
from import_data.comparisions.file_writers.write_to_file_teams import write_to_file
from utilities.dbconnect import DatabaseConnection


def make_offensive_comparisons(ty_uids, year, possible_comps, driver_logger):
    comparisons = {}
    for ty_uid in ty_uids:
        comparisons[ty_uid] = determine_comp(ty_uid, year, possible_comps, driver_logger)
    write_to_file(comparisons, "offense")


def determine_comp(ty_uid, year, teams_to_compare, driver_logger):
    comp = None
    year_pa, year_totals = get_year_totals(year, driver_logger)
    team_pa, stats = get_offensive_stats(ty_uid, driver_logger)
    team_drs = offensive_dr_calc(team_pa, stats, year_pa, year_totals, driver_logger)
    if len(team_drs) > 0:
        comp = find_comp(ty_uid, team_drs, teams_to_compare, driver_logger)
    return comp


def offensive_dr_calc(pa, stats, year_pa, year_stats, driver_logger):
    team_drs = {}
    for key, value in stats.items():
        if value != -1:
            try:
                dr_stat = (value / pa) / (year_stats[key] / year_pa)
                if dr_stat > 0.0:
                    team_drs[key] = dr_stat
            except KeyError:
                continue
            except ZeroDivisionError:
                continue
    return team_drs


def find_comp(ty_uid, team_drs, teams_to_compare, driver_logger):
    comp_team_scores = {}
    for comp_ty_uid, possible_comps_stats in teams_to_compare.items():
        if comp_ty_uid != ty_uid:
            if len(possible_comps_stats) == 8:
                for stat_name, stat_value in possible_comps_stats.items():
                    comp_team_scores[comp_ty_uid] = 0
                    try:
                        comp_team_scores[comp_ty_uid] += abs(team_drs[stat_name] - stat_value)
                    except KeyError:
                        continue
    return sorted(comp_team_scores.items(), key=lambda kv: kv[1])[0][0]
