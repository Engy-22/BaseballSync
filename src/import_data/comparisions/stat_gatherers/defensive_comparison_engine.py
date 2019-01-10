from import_data.comparisions.getters.defensive_team_getters import get_year_totals, get_defensive_stats
from import_data.comparisions.file_writers.write_to_file_teams import write_to_file
from utilities.DB_Connect import DB_Connect


def make_defensive_comparisons(ty_uids, year, possible_comps):
    comparisons = {}
    db, cursor = DB_Connect.grab("baseballData")
    for ty_uid in ty_uids:
        comparisons[ty_uid] = determine_comp(ty_uid, year, possible_comps)
    DB_Connect.close(db)
    write_to_file(comparisons, "defense")


def determine_comp(ty_uid, year, teams_to_compare):
    comp = None
    year_pa, year_totals = get_year_totals(year)
    team_pa, stats = get_defensive_stats(ty_uid)
    team_drs = defensive_dr_calc(team_pa, stats, year_pa, year_totals)
    if len(team_drs) > 0:
        comp = find_comp(ty_uid, team_drs, teams_to_compare)
    return comp


def defensive_dr_calc(pa, stats, year_pa, year_stats):
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


def find_comp(ty_uid, team_drs, teams_to_compare):
    comp_team_scores = {}
    for comp_ty_uid, possible_comps_stats in teams_to_compare.items():
        if comp_ty_uid != ty_uid:
            if len(possible_comps_stats) == 7:
                for stat_name, stat_value in possible_comps_stats.items():
                    comp_team_scores[comp_ty_uid] = 0
                    try:
                        comp_team_scores[comp_ty_uid] += abs(team_drs[stat_name] - stat_value)
                    except KeyError:
                        continue
    return sorted(comp_team_scores.items(), key=lambda kv: kv[1])[0][0]
