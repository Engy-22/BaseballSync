def determine_direction(event, bats_with):
    if 'grounds out' in event:
        try:
            location = event.split('grounds out')[1].split(', ')[1].split(' ')[0]
        except IndexError:
            location = event.split('grounds out')[1].split('to ')[1].split(' ')[0]
    elif 'grounds into a force out' in event:
        location = event.split(', ')[1].split(' ')[0]
    elif 'flies out' in event:
        try:
            location = event.split('flies out to ')[1].split(' ')[0]
        except IndexError:
            location = event.split('flies out')[1].split('to ')[1].split(' ')[0]
    elif 'pops out' in event:
        try:
            location = event.split('pops out to ')[1].split(' ')[0]
        except IndexError:
            location = event.split('pops out')[1].split('to ')[1].split(' ')[0]
    elif 'lines out' in event:
        try:
            location = event.split('lines out to ')[1].split(' ')[0]
        except IndexError:
            location = event.split('lines out')[1].split('to ')[1].split(' ')[0]
    elif 'singles' in event or 'doubles' in event or 'triples' in event or 'homers' in event:
        try:
            location = event.split('to ')[1].split(' ')[0]
        except IndexError:
            if 'hit by batted ball' in event:
                location = event.split(', ')[1].split(' ')[0]
            else:
                location = 'unknown'
                print('\t\t\t' + event)
    elif 'sacrifice bunt, ' in event:
        location = event.split('sacrifice bunt, ')[1].split(' ')[0]
    elif 'sacrifice fly' in event:
        if 'error' not in event:
            location = event.split('sacrifice fly to ')[1].split(' ')[0]
        else:
            location = event.split('error by ')[1].split(' ')[0]
    elif 'double play' in event:
        location = event.split(', ')[1].split(' ')[0]
    elif 'walks' in event or 'strikes out' in event or 'called out on strikes' in event:
        location = "none"
    else:
        raise Exception
        location = "none"
    try:
        directions = {'r': {'third': 'pulled', 'second': 'oppo', 'first': 'oppo', 'shortstop': 'pulled',
                            'pitcher': 'middle', 'right': 'oppo', 'center': 'middle', 'left': 'pulled'},
                      'l': {'third': 'oppo', 'second': 'pulled', 'first': 'pulled', 'shortstop': 'oppo',
                            'pitcher': 'middle', 'right': 'pulled', 'center': 'middle', 'left': 'oppo'}}
        return directions[bats_with][location]
    except KeyError:
        return "none"
