def determine_field(event, description):
    outcomes = {'so': 'none', 'hr': 'of', 'go': 'if', 'fo': 'of', 'po': 'if', 'bb': 'none', 'hbp': 'none', 'sh': 'if',
                'ibb': 'none', 'gdp': 'if', 'sf': 'of'}
    try:
        return outcomes[event]
    except KeyError:
        if event in ['1b', '2b', '3b', 'hr']:
            position = description.split('to ')[1].split(' ')[0]
            if position in ['left', 'center', 'right']:
                return 'of'
            else:
                return 'if'
        else:
            raise KeyError
