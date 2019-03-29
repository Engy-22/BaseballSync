from import_data.comparisions.getters.hitting_getters import get_hitter_stats
from import_data.comparisions.file_writers.write_to_file_players import write_to_file


def make_hitter_comparisons(hitters, year, possible_comps, year_pa, year_totals, logger):
    logger.log('\t\t\tMaking comparisons for ' + str(year) + ' hitters')
    comparisons = {}
    for hitter in hitters:
        comparisons[hitter] = determine_comp(hitter, year, possible_comps, year_pa, year_totals)
    write_to_file(year, comparisons, "batting")


def determine_comp(hitter, year, hitters_to_compare, year_pa, year_totals):
    hitter_pa, stats = get_hitter_stats(hitter, year)
    return find_comp(hitter, year, hitter_dr_calc(hitter_pa, stats, year_pa, year_totals), hitters_to_compare)


def hitter_dr_calc(pa, stats, year_pa, year_stats):
    hitter_drs = {}
    for key, value in stats.items():
        try:
            dr_stat = (value / pa) / (year_stats[key] / year_pa)
            if dr_stat > 0.0:
                hitter_drs[key] = dr_stat
        except (KeyError, ZeroDivisionError):
            continue
    return hitter_drs


def find_comp(hitter, year, hitter_drs, hitters_to_compare):
    if len(hitter_drs) > 0:
        comp_player_scores = {}
        for year_to_compare, possible_comps in hitters_to_compare.items():
            for possible_comp_hitter, possible_comp_stats in possible_comps.items():
                if possible_comp_hitter != hitter + ';' + str(year):
                    if len(possible_comp_stats) > 0:
                        stat_count = 0
                        comp_player_scores[possible_comp_hitter] = 0
                        for statistic, value in possible_comp_stats.items():
                            try:
                                comp_player_scores[possible_comp_hitter] += abs(hitter_drs[statistic] - value)
                                stat_count += 1
                            except KeyError:
                                continue
                        comp_player_scores[possible_comp_hitter] /= stat_count
        return sorted(comp_player_scores.items(), key=lambda kv: kv[1])[0][0]
    else:
        return None
