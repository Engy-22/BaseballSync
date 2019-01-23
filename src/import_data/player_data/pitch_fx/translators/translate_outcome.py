def translate_pitch_outcome(outcome, description):
    outcomes = {'Single': '1b', 'Double': '2b', 'Triple': '3b', 'Strikeout': 'so', 'Home Run': 'hr', 'Groundout': 'go',
                'Flyout': 'fo', 'Lineout': 'lo', 'Pop Out': 'po', 'Walk': 'bb', 'Hit By Pitch': 'hbp',
                'Sac Bunt': 'sh', 'Intent Walk': 'ibb', 'Forceout': 'fc', 'Grounded Into DP': 'gdp',
                'Double Play': 'dp', 'Field Error': 'error', 'Runner Out': 'cs', 'Sac Fly': 'sf',
                'Bunt Groundout': 'go', 'Fielders Choice': 'fc', 'Fielders Choice Out': 'fc'}
    try:
        this_outcome = outcomes[outcome]
    except KeyError:
        if outcome == 'Fan interference':
            if 'single' in description:
                this_outcome = '1b'
            elif 'double' in description:
                this_outcome = '2b'
            elif 'triple' in description:
                this_outcome = '3b'
            else:
                this_outcome = 'hr'
        else:
            this_outcome = "none"
    return this_outcome
