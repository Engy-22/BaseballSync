from utilities.DB_Connect import DB_Connect
import urllib.request
from bs4 import BeautifulSoup as bs
from collections import Counter


def get_year_data(data_year):
    print('adding to years: league averages and totals')
    mlb_schedule = str(bs(urllib.request.urlopen('https://www.baseball-reference.com/leagues/MLB/' + str(data_year)
                                                 + '-schedule.shtml'), 'html.parser'))
    opening_date = mlb_schedule.split('<h3>')[1].split('</h3>')[0]
    months = {'March': '03', 'April': '04'}
    opening_day = months[opening_date.split(', ')[1].split(' ')[0]] + '-'\
                  + opening_date.split(', ')[1].split(' ')[1].split(',')[0]
    batting_list = ['PA', 'AB', 'R', 'H', '2B', '3B', 'HR', 'RBI', 'SB', 'BB', 'SO', 'batting_avg', 'onbase_perc',
                    'slugging_perc', 'onbase_plus_slugging']
    pitching_list = ['earned_run_avg', 'whip', 'strikeouts_per_nine', 'strikeouts_per_base_on_balls']
    fielding_list = ['E_def', 'fielding_perc']
    stats = ['AB', 'H', '2B', '3B', 'HR']
    stat_translate = {'AB': 'AB', 'H': 'H', '2B': 'double', '3B': 'triple', 'HR': 'homerun'}
    r_location = {'Up Mdle': 'centerfield', 'Opp Fld': 'rightfield', 'Pulled': 'leftfield'}
    l_location = {'Up Mdle': 'centerfield', 'Opp Fld': 'leftfield', 'Pulled': 'rightfield'}
    location = {'AB_centerfield': 0, 'AB_leftfield': 0, 'AB_rightfield': 0,
                'H_centerfield': 0, 'H_leftfield': 0, 'H_rightfield': 0,
                'double_centerfield': 0, 'double_leftfield': 0, 'double_rightfield': 0,
                'triple_centerfield': 0, 'triple_leftfield': 0, 'triple_rightfield': 0,
                'homerun_centerfield': 0, 'homerun_leftfield': 0, 'homerun_rightfield': 0}
    location_rows = str(bs(urllib.request.urlopen('https://www.baseball-reference.com/leagues/split.cgi?t=b&lg=MLB&year'
                                                  + '=2018'), "html.parser")).split('<tr >')
    ip_er_year = str(bs(urllib.request.urlopen('https://www.baseball-reference.com/leagues/split.cgi?t=p&lg=MLB&year='
                                               + '2018'), "html.parser")).split('<h2>Leverage</h2>')[0]\
    .split('<h2>Season Totals -- Game-Level</h2>')[1].split('</thead>')[1].split('<tr >')[1]
    er = ip_er_year.split('data-stat="ER" >')[1].split('<')[0]
    ip = ip_er_year.split('data-stat="IP" >')[1].split('<')[0]
    try:
        for row in location_rows:
            if '-RHB' in row:
                handedness = "R"
            elif '-LHB' in row:
                handedness = "L"
            else:
                continue
            for stat in stats:
                if handedness == "R":
                    location[stat_translate[stat] + '_' + r_location[row.split('data-stat="split_name" '
                                                                               + '>')[1].split('-')[0]]] += \
                    int(row.split('data-stat="' + stat + '" >')[1].split('<')[0])
                else:
                    location[stat_translate[stat] + '_' + l_location[row.split('data-stat="split_name" '
                                                                               + '>')[1].split('-')[0]]] += \
                    int(row.split('data-stat="' + stat + '" >')[1].split('<')[0])
    except IndexError:
        location = {'AB_centerfield': -1, 'AB_leftfield': -1, 'AB_rightfield': -1,
                    'H_centerfield': -1, 'H_leftfield': -1, 'H_rightfield': -1,
                    'double_centerfield': -1, 'double_leftfield': -1, 'double_rightfield': -1,
                    'triple_centerfield': -1, 'triple_leftfield': -1, 'triple_rightfield': -1,
                    'homerun_centerfield': -1, 'homerun_leftfield': -1, 'homerun_rightfield': -1}
    field_list = ""
    value_list = ""
    for key, value in location.items():
        field_list += key + ', '
        value_list += str(value) + ', '
    db, cursor = DB_Connect.grab("baseballData")
    if len(DB_Connect.read(cursor, "select * from years where year = " + str(data_year) + ";")) == 0:
        DB_Connect.write(db, cursor, "insert into years (year,opening_day,G,PA,AB,R,H,2B,3B,HR,RBI,SB,BB,SO,BA,OBP,SLG,"
                                     "OPS,ER,IP,ERA,WHIP,k_9,k_bb, E, F_percent, " + field_list[:-2] + ") values ("
                                     + str(data_year) + ',"' + opening_day + '",' + leg_work(data_year, "batting",
                                     batting_list) + "," + er + "," + ip + leg_work(data_year, "pitching", pitching_list)
                                     + leg_work(data_year, "fielding", fielding_list) + ", " + value_list[:-2] + ");")
    DB_Connect.close(db)


def leg_work(year, stat_type, guide_dictionary):
    lat_line1 = str(bs(urllib.request.urlopen("https://www.baseball-reference.com/leagues/MLB/" + str(year)
                                              + "-standard-" + stat_type + ".shtml"), "html.parser"))
    lat_line2 = lat_line1.split('</tbody>')[1].split('</tfoot>')[0]
    this_string = ""
    for i in lat_line2.split('<td'):
        if i.split('data-stat="')[1].split('">')[0] in guide_dictionary:
            this_stat = i.split('">')[1].split('<')[0]
            this_string += ',' + this_stat if this_stat != '' else ',-1'
    if stat_type == 'batting':
        big_table = lat_line1.split('</tbody>')[0].split('<tbody>')[1].split('<tr')[1:-1]
        games_list = []
        for row in big_table:
            games_list.append(row.split('data-stat="G">')[1].split('<')[0])
        games = Counter(games_list).most_common(1)[0][0]
        this_string = str(games) + this_string
    return this_string


# for year in range(2018, 1875, -1):
#     print(str(year))
#     league_averages_and_totals(year)
# get_year_data(2018)
