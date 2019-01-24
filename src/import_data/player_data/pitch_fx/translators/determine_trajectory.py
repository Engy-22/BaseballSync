def determine_trajectory(event, description):
    outcomes = {'so': 'none', 'go': 'gb', 'fo': 'fb', 'lo': 'ld', 'po': 'fb', 'bb': 'none', 'hbp': 'none', 'sh': 'gb',
                'ibb': 'none', 'gdp': 'gb', 'sf': 'fb'}
    try:
        trajectory = outcomes[event]
    except KeyError:
        outcomes = {'line': 'ld', 'fly': 'fb', 'ground': 'gb', 'pop': 'fb'}
        if event in ['1b', '2b', '3b', 'hr']:
            try:
                trajectory = outcomes[description.split('on a ')[1].split(' ')[0]]
            except KeyError:
                trajectory = outcomes[description.split('on a ')[1].split(' ')[1]]
        elif event == 'fc':
            if description.split(' into ')[0].split(' ')[-1] == 'grounds':
                trajectory = 'gb'
            elif description.split(' into ')[0].split(' ')[-1] == 'flies':
                trajectory = 'fb'
            elif description.split(' into ')[0].split(' ')[-2] == 'ground':
                trajectory = 'gb'
            elif description.split(' into ')[0].split(' ')[-1] == 'lines':
                trajectory = 'ld'
            elif description.split(' into ')[0].split(' ')[-1] == 'pops':
                trajectory = 'fb'
            elif 'reaches' in description:
                fielders = {'first': 'gb', 'second': 'gb', 'third': 'gb', 'shortstop': 'gb', 'left': 'fb',
                            'right': 'fb', 'center': 'fb'}
                try:
                    trajectory = fielders[description.split('fielded by ')[1].split(' ')[0]]
                except IndexError:
                    trajectory = fielders[description.split(', ')[1].split(' ')[0]]
            else:
                print('\nasdf')
                print(event)
                print(description)
        elif event == 'dp':
            if description.split(' into ')[0].split(' ')[-1] == 'lines':
                trajectory = 'ld'
            elif description.split(' into ')[0].split(' ')[-1] == 'flies':
                trajectory = 'fb'
            elif description.split(' into ')[0].split(' ')[-1] == 'pops':
                trajectory = 'fb'
            elif description.split(' into ')[0].split(' ')[-1] == 'grounds':
                trajectory = 'gb'
            elif "fielder's choice" in description:
                trajectory = 'gb'
            elif 'challenged' in description:
                return determine_trajectory(event, description.split(':')[1])
            else:
                print('\n;lkj')
                print(event)
                print(description)
                print(description.split(' into ')[0].split(' ')[-1])
        elif event == 'error':
            error = event.split('by')[1].split(' ')[0]
            if error in ['left', 'center', 'right']:
                trajectory = 'fb'
            else:
                trajectory = 'gb'
        else:
            if event != 'cs':
                print('\nasdfasdf')
                print(event)
                print(description)
            trajectory = "none"
    return trajectory
