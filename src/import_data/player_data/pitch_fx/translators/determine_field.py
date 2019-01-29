def determine_field(event, description):
    outcomes = {'so': 'none', 'hr': 'of', 'go': 'if', 'fo': 'of', 'po': 'if', 'bb': 'none', 'hbp': 'none', 'sh': 'if',
                'ibb': 'none', 'gdp': 'if', 'sf': 'of', 'cs': 'none'}
    try:
        return outcomes[event]
    except KeyError:
        if event in ['1b', '2b', '3b', 'hr', 'lo']:
            if description.split(' to ')[1].split(' ')[0] in ['left', 'center', 'right']:
                return 'of'
            else:
                return 'if'
        elif event == 'fc':
            if description.split(', ')[1].split(' ')[0] in ['left', 'center', 'right']:
                return 'of'
            else:
                return 'if'
        elif event == 'dp':
            if description.split(', ')[1].split(' ')[0] in ['left', 'center', 'right']:
                return 'of'
            else:
                return 'if'
        elif event == 'error':
            if description.split(' error by ')[1].split(' ')[0] in ['left', 'center', 'right']:
                return 'of'
            else:
                return 'if'
        else:
            print(event, description)
            raise KeyError
