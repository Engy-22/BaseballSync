def determine_field(event, description):
    outcomes = {'so (swinging)': 'none', 'so (looking)': 'none', 'hr': 'outfield', 'go': 'infield', 'fo': 'outfield', 'po': 'infield',
                'bb': 'none', 'hbp': 'none', 'sh': 'infield', 'ibb': 'none', 'gdp': 'infield', 'sf': 'outfield', 'cs': 'none'}
    try:
        return outcomes[event]
    except KeyError:
        if event in ['1b', '2b', '3b', 'hr', 'lo']:
            if 'hit by batted ball' not in description:
                try:
                    if description.split(' to ')[1].split(' ')[0] in ['left', 'center', 'right']:
                        return 'outfield'
                    else:
                        return 'infield'
                except IndexError:
                    try:
                        if description.split(' down the ')[1].split('-')[0] in ['left', 'center', 'right']:
                            return 'outfield'
                        else:
                            return 'infield'
                    except IndexError:
                        'unknown'
            else:
                return 'infield'
        elif event == 'fc':
            if description.split(', ')[1].split(' ')[0] in ['left', 'center', 'right']:
                return 'outfield'
            else:
                return 'infield'
        elif event in ['dp', '3p']:
            if description.split(', ')[1].split(' ')[0] in ['left', 'center', 'right']:
                return 'outfield'
            else:
                return 'infield'
        elif event == 'error':
            if 'catcher interference' not in description:
                if description.split(' error by ')[1].split(' ')[0] in ['left', 'center', 'right']:
                    return 'outfield'
                else:
                    return 'infield'
            else:
                return 'none'
        else:
            print(event, description)
            print('\t\t\tasdfasdfasdfasdf')
            raise KeyError
