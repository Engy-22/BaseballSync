def determine_field(event):
    outcomes = {'so': 'none', 'hr': 'of', 'go': 'if', 'fo': 'of', 'po': 'if', 'bb': 'none', 'hbp': 'none', 'sh': 'if',
                'ibb': 'none', 'gdp': 'if', 'sf': 'of'}
    try:
        return outcomes[event]
    except KeyError:
        return "none"
