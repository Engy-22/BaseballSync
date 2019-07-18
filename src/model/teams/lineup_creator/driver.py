import os
from model.teams.lineup_creator.reorganize_data import reorganize_batting_spots
from model.teams.lineup_creator.check_existing_lineup import player_available, position_available, sp_can_bat_here
from random import randint
from utilities.logger import Logger
from utilities.time_converter import time_converter
from utilities.properties import log_prefix
import time

logger = Logger(os.path.join(log_prefix, "controller", "create_lineup.log"))


def create_lineup(team_id, year, roster, team_info, starting_pitcher, opposing_pitcher, use_dh):
    start_time = time.time()
    logger.log('Creating lineup for ' + team_id + ' (' + str(year) + ')')
    if not opposing_pitcher.get_throwing_handedness():
        opposing_pitcher.set_throwing_handedness()
    batting_order = []
    position_list = []
    place = 1
    while place <= 9:
        logger.log('\tPlace ' + str(place))
        if not use_dh and place == 9 and 'SP' not in position_list:
            logger.log('\t\tAdding starting pitcher')
            batting_order.append(starting_pitcher)
            position_list.append('SP')
        else:
            batter, position = get_player_randomly(roster, team_info, place, position_list,
                                                   opposing_pitcher.get_throwing_handedness(), use_dh)
            if not player_available(batting_order, batter) or position is None:
                batter, position = get_player_incrementally(roster, team_info, batting_order, place, position_list,
                                                            use_dh, opposing_pitcher.get_throwing_handedness())
                if batter is None:
                    batting_order = []
                    position_list = []
                    place = 1
                    logger.log('\n---------restart---------\n')
                    continue
            if position == 'SP':
                batting_order.append(starting_pitcher)
            else:
                batting_order.append(batter)
            position_list.append(position)
        place += 1
    logger.log('Time = ' + time_converter(time.time() - start_time) + '\n\n')
    return batting_order, position


def get_player_randomly(roster, team_info, spot, position_list, opposing_pitcher_handedness, use_dh):
    logger.log('\t\tGetting player randomly')
    options = reorganize_batting_spots(roster, team_info, spot, opposing_pitcher_handedness)
    games = 0
    for player, starts in options.items():
        games += starts
    picker = randint(1, games)
    temp_count = 0
    for player, starts in options.items():
        if picker <= temp_count + starts:
            if not player.get_year_positions():
                player.set_year_positions(team_info['player_positions'][player.get_player_id()])
            return player, get_position_incrementally(player, team_info, position_list, use_dh,
                                                      opposing_pitcher_handedness, spot)
        else:
            temp_count += starts


def get_player_incrementally(roster, team_info, batting_order, spot, position_list, use_dh,
                             opposing_pitcher_handedness, index=0):
    logger.log('\t\tGetting player incrementally')
    try:
        player = sorted(reorganize_batting_spots(roster, team_info, spot, opposing_pitcher_handedness).items(),
                        key=lambda kv: kv[1], reverse=True)[index][0]
    except IndexError:
        return None, None
    position = get_position_incrementally(player, team_info, position_list, use_dh, opposing_pitcher_handedness, spot)
    if not(player_available(batting_order, player)) or position is None:
        return get_player_incrementally(roster, team_info, batting_order, spot, position_list, use_dh,
                                        opposing_pitcher_handedness, index+1)
    else:
        return player, position


def get_position_incrementally(player, team_info, position_list, use_dh, place, opposing_pitcher_handedness, index=0):
    logger.log('\t\t\tGetting position incrementally')
    if not player.get_year_positions():
        player.set_year_positions(team_info['player_positions'][player.get_player_id()])
    try:
        new_position = player.get_year_positions()[index]
        if position_available(position_list, new_position) and new_position != 'RP':
            if use_dh:
                if new_position != 'SP':
                    return new_position
                else:
                    return get_position_incrementally(player, team_info, position_list, use_dh, place,
                                                      opposing_pitcher_handedness, index + 1)
            else:
                if new_position != 'DH' or (new_position == 'SP' and sp_can_bat_here(player, place,
                                                                                     opposing_pitcher_handedness)):
                    return new_position
                else:
                    return get_position_incrementally(player, team_info, position_list, use_dh, place,
                                                      opposing_pitcher_handedness, index + 1)
        else:
            return get_position_incrementally(player, team_info, position_list, use_dh, place, opposing_pitcher_handedness,
                                              index + 1)
    except IndexError:
        return None
