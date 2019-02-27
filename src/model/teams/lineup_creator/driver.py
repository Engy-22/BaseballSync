from model.teams.lineup_creator.get_starting_pitcher import get_starting_pitcher
from model.teams.lineup_creator.reorganize_data import reorganize_batting_spots
from model.teams.lineup_creator.check_existing_lineup import player_available, position_available, sp_can_bat_here
from random import randint
from utilities.logger import Logger
from utilities.time_converter import time_converter
import time

logger = Logger("C:\\Users\\Anthony Raimondo\\PycharmProjects\\baseball-sync\\logs\\controller\\create_lineup.log")


def create_lineup(team_id, year, roster, game_num, use_dh):
    start_time = time.time()
    logger.log('Creating lineup for ' + team_id + ' (' + str(year) + ')')
    starting_pitcher = get_starting_pitcher(team_id, year, game_num)
    batting_order = []
    position_list = []
    # print('\n')
    place = 1
    while place <= 9:
        logger.log('\tPlace ' + str(place))
        if not use_dh and place == 9 and 'SP' not in position_list:
            logger.log('\t\tAdding starting pitcher')
            batting_order.append(starting_pitcher)
            position_list.append('SP')
        else:
            batter, position = get_player_randomly(roster, place, position_list, use_dh)
            if not player_available(batting_order, batter) or position is None:
                batter, position = get_player_incrementally(roster, batting_order, place, position_list, use_dh)
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
    # for i in range(9):
    #     print(batting_order[i].get_player_id() + '\t' + str(position_list[i]))
    logger.log('Time = ' + time_converter(time.time() - start_time) + '\n\n')


def get_player_randomly(roster, spot, position_list, use_dh):
    logger.log('\t\tGetting player randomly')
    options = reorganize_batting_spots(roster, spot)
    games = 0
    for player, starts in options.items():
        games += starts
    picker = randint(1, games)
    temp_count = 0
    for player, starts in options.items():
        if picker <= temp_count + starts:
            return player, get_position_incrementally(player, position_list, player.get_year_positions(), use_dh, spot)
        else:
            temp_count += starts


def get_player_incrementally(roster, batting_order, spot, position_list, use_dh, index=0):
    logger.log('\t\tGetting player incrementally')
    try:
        player = sorted(reorganize_batting_spots(roster, spot).items(), key=lambda kv: kv[1], reverse=True)[index][0]
    except IndexError:
        return None, None
    position = get_position_incrementally(player, position_list, player.get_year_positions(), use_dh, spot)
    if not(player_available(batting_order, player)) or position is None:
        return get_player_incrementally(roster, batting_order, spot, position_list, use_dh, index+1)
    else:
        return player, position


def get_position_incrementally(player, position_list, player_position_options, use_dh, place, index=0):
    logger.log('\t\t\tGetting position incrementally')
    try:
        new_position = player_position_options[index]
        if position_available(position_list, new_position) and new_position != 'RP':
            if use_dh:
                if new_position != 'SP':
                    return new_position
                else:
                    return get_position_incrementally(player, position_list, player_position_options, use_dh, place,
                                                      index + 1)
            else:
                if new_position != 'DH' or (new_position == 'SP' and sp_can_bat_here(player, place)):
                    return new_position
                else:
                    return get_position_incrementally(player, position_list, player_position_options, use_dh, place,
                                                      index + 1)
        else:
            return get_position_incrementally(player, position_list, player_position_options, use_dh, place, index + 1)
    except IndexError:
        return None
