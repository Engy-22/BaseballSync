from import_data.comparisions.getters.pitching_getters import get_pitcher_stats
from import_data.comparisions.file_writers.write_to_file_players import write_to_file


def make_pitcher_comparisons(pitchers, year, possible_comps, year_pa, year_totals, logger):
    logger.log('\t\t\tMaking comparisons for ' + str(year) + ' pitchers')
    comparisons = {}
    for pitcher in pitchers:
        comparisons[pitcher] = determine_comp(pitcher, year, possible_comps, year_pa, year_totals)
    write_to_file(year, comparisons, "pitching")


def determine_comp(pitcher, year, pitchers_to_compare, year_pa, year_totals):
    pitcher_pa, stats = get_pitcher_stats(pitcher, year)
    return find_comp(pitcher, year, pitcher_dr_calc(pitcher_pa, stats, year_pa, year_totals), pitchers_to_compare)


def pitcher_dr_calc(pa, stats, year_pa, year_stats):
    pitcher_drs = {}
    for key, value in stats.items():
        try:
            dr_stat = (value / pa) / (year_stats[key] / year_pa)
            if dr_stat > 0.0:
                pitcher_drs[key] = dr_stat
        except (KeyError, ZeroDivisionError):
            continue
    return pitcher_drs


def find_comp(pitcher, year, pitcher_drs, pitchers_to_compare):
    if len(pitcher_drs) > 0:
        comp_player_scores = {}
        for year_to_compare, possible_comps in pitchers_to_compare.items():
            for possible_comp_pitcher, possible_comp_stats in possible_comps.items():
                if possible_comp_pitcher != pitcher + ';' + str(year):
                    if len(possible_comp_stats) > 0:
                        stat_count = 0
                        comp_player_scores[possible_comp_pitcher] = 0
                        for statistic, value in possible_comp_stats.items():
                            try:
                                comp_player_scores[possible_comp_pitcher] += abs(pitcher_drs[statistic] - value)
                                stat_count += 1
                            except KeyError:
                                continue
                        comp_player_scores[possible_comp_pitcher] /= stat_count
        return sorted(comp_player_scores.items(), key=lambda kv: kv[1])[0][0]
    else:
        return None
