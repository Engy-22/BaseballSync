from import_data.comparisions.getters.offensive_team_getters import get_offensive_stats
from import_data.comparisions.file_writers.write_to_file_teams import write_to_file


def make_offensive_comparisons(ty_uids, possible_comps, year_pa, year_totals, logger):
    logger.log('\t\t\tMaking team offensive comparisons')
    comparisons = {}
    for ty_uid in ty_uids:
        comparisons[ty_uid] = determine_comp(ty_uid, possible_comps, year_pa, year_totals)
    write_to_file(comparisons, "offense")


def determine_comp(ty_uid, teams_to_compare, year_pa, year_totals):
    team_pa, stats = get_offensive_stats(ty_uid)
    return find_comp(ty_uid, offensive_dr_calc(team_pa, stats, year_pa, year_totals), teams_to_compare)


def offensive_dr_calc(pa, stats, year_pa, year_stats):
    team_drs = {}
    for key, value in stats.items():
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
            if len(possible_comps_stats) > 0:
                stat_count = 0
                comp_team_scores[comp_ty_uid] = 0
                for stat_name, stat_value in possible_comps_stats.items():
                    try:
                        comp_team_scores[comp_ty_uid] += abs(team_drs[stat_name] - stat_value)
                        stat_count += 1
                    except KeyError:
                        continue
                comp_team_scores[comp_ty_uid] /= stat_count
    return sorted(comp_team_scores.items(), key=lambda kv: kv[1])[0][0]
