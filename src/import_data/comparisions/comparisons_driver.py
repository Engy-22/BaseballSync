from import_data.comparisions.stat_gatherers.gather_players import gather_players
from import_data.comparisions.stat_gatherers.gather_teams import gather_teams
from utilities.get_most_recent_year import get_most_recent_year
from import_data.comparisions.stat_gatherers.batting_comparison_engine import make_hitter_comparisons, hitter_dr_calc
from import_data.comparisions.getters.hitting_getters import get_year_totals as hitter_year_totals, get_hitter_stats
from import_data.comparisions.stat_gatherers.offensive_comparison_engine import make_offensive_comparisons, offensive_dr_calc
from import_data.comparisions.getters.offensive_team_getters import get_offensive_stats
from import_data.comparisions.getters.pitching_getters import get_year_totals as pitcher_year_totals, get_pitcher_stats
from import_data.comparisions.stat_gatherers.pitching_comparison_engine import make_pitcher_comparisons, pitcher_dr_calc
from import_data.comparisions.stat_gatherers.defensive_comparison_engine import make_defensive_comparisons, defensive_dr_calc
from import_data.comparisions.getters.defensive_team_getters import get_defensive_stats, get_year_totals as defensive_year_totals


def comparisons_driver():
    possible_hitter_comps = {}
    hitters_to_compare = {}
    possible_pitcher_comps = {}
    pitchers_to_compare = {}
    possible_offensive_comps = {}
    offenses_to_compare = {}
    possible_defensive_comps = {}
    defenses_to_compare = {}

    print('making hitter comparisons (overall)')
    hitter_years_to_compare = [year for year in range(get_most_recent_year(), 1997, -1)]
    for year_to_compare in hitter_years_to_compare:
        possible_hitter_comps[year_to_compare] = {}
        year_pa, year_totals = hitter_year_totals(year_to_compare)
        hitters_to_compare[year_to_compare] = gather_players(year_to_compare, "batting")
        for comp_ty_uid in hitters_to_compare[year_to_compare]:
            comp_team_pa, comp_stats = get_hitter_stats(comp_ty_uid, year_to_compare)
            if comp_team_pa >= 300:
                possible_hitter_comps[year_to_compare][comp_ty_uid + ';' + str(year_to_compare)] =\
                    hitter_dr_calc(comp_team_pa, comp_stats, year_pa, year_totals)
    for comp_year in range(1876, get_most_recent_year() + 1, 1):
        make_hitter_comparisons(gather_players(comp_year, "batting"), comp_year, possible_hitter_comps)

    print('making pitcher comparisons (overall)')
    pitcher_years_to_compare = [year for year in range(get_most_recent_year(), 1996, -1)]
    for year_to_compare in pitcher_years_to_compare:
        possible_pitcher_comps[year_to_compare] = {}
        year_pa, year_totals = pitcher_year_totals(year_to_compare)
        pitchers_to_compare[year_to_compare] = gather_players(year_to_compare, "pitching")
        for comp_pitcher in pitchers_to_compare[year_to_compare]:
            comp_pitcher_pa, comp_stats = get_pitcher_stats(comp_pitcher, year_to_compare)
            if comp_pitcher_pa >= 300:
                possible_pitcher_comps[year_to_compare][comp_pitcher + ';' + str(year_to_compare)] = \
                    pitcher_dr_calc(comp_pitcher_pa, comp_stats, year_pa, year_totals)
    for comp_year in range(1876, get_most_recent_year() + 1, 1):
        make_pitcher_comparisons(gather_players(comp_year, "pitching"), comp_year, possible_pitcher_comps)

    print('making team offensive comparisons')
    offensive_years_to_compare = [year for year in range(get_most_recent_year(), 1997, -1)]
    for year_to_compare in offensive_years_to_compare:
        year_pa, year_totals = hitter_year_totals(year_to_compare)
        offenses_to_compare[year_to_compare] = gather_teams(year_to_compare)
        for comp_ty_uid in offenses_to_compare[year_to_compare]:
            comp_team_pa, comp_stats = get_offensive_stats(comp_ty_uid)
            possible_offensive_comps[comp_ty_uid] = offensive_dr_calc(comp_team_pa, comp_stats, year_pa, year_totals)
    for comp_year in range(1876, get_most_recent_year() + 1, 1):
        make_offensive_comparisons(gather_teams(comp_year), comp_year, possible_offensive_comps)

    print('making team defensive comparisons')
    defensive_years_to_compare = [year for year in range(get_most_recent_year(), 1997, -1)]
    for year_to_compare in defensive_years_to_compare:
        year_pa, year_totals = defensive_year_totals(year_to_compare)
        defenses_to_compare[year_to_compare] = gather_teams(year_to_compare)
        for comp_ty_uid in defenses_to_compare[year_to_compare]:
            comp_team_pa, comp_stats = get_defensive_stats(comp_ty_uid)
            possible_defensive_comps[comp_ty_uid] = defensive_dr_calc(comp_team_pa, comp_stats, year_pa, year_totals)
    for comp_year in range(1876, get_most_recent_year() + 1, 1):
        make_defensive_comparisons(gather_teams(comp_year), comp_year, possible_defensive_comps)


# start_time = time.time()
# comparisons_driver()
# print((time.time() - start_time) / 3600)
