def determine_trajectory(event, description):
    trajectories = {'so': 'none', 'go': 'gb', 'fo': 'fb', 'lo': 'ld', 'po': 'fb', 'bb': 'none', 'hbp': 'none',
                    'sh': 'gb', 'ibb': 'none', 'gdp': 'gb', 'sf': 'fb'}
    try:
        trajectory = trajectories[event]
    except KeyError:
        trajectories = {'line': 'ld', 'fly': 'fb', 'ground': 'gb', 'pop': 'fb'}
        if event in ['1b', '2b', '3b', 'hr']:
            if 'grand slam' not in description and 'bunt' not in description:
                try:
                    trajectory = trajectories[description.split('on a ')[1].split(' ')[0]]
                except KeyError:
                    try:
                        trajectory = trajectories[description.split('on a ')[1].split(' ')[1]]
                    except KeyError:
                        trajectory = 'unknown'
                except IndexError:
                    trajectory = 'unknown'
            else:
                if 'bunt' in description:
                    trajectory = 'gb'
                else:
                    trajectory = trajectories['fly']
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
                            'right': 'fb', 'center': 'fb', 'catcher': 'gb', 'pitcher': 'gb'}
                try:
                    trajectory = fielders[description.split('fielded by ')[1].split(' ')[0]]
                except IndexError:
                    try:
                        trajectory = fielders[description.split(', ')[1].split(' ')[0]]
                    except KeyError:
                        trajectory = fielders[description.split(':')[1].split(', ')[1].split(' ')[0]]
            else:
                print('\nasdf')
                print(event)
                print(description)
        elif event in ['dp', '3p']:
            if description.split(' into ')[0].split(' ')[-1] == 'lines':
                trajectory = 'ld'
            elif description.split(' into ')[0].split(' ')[-1] == 'flies':
                trajectory = 'fb'
            elif description.split(' into ')[0].split(' ')[-1] == 'pops':
                trajectory = 'fb'
            elif description.split(' into ')[0].split(' ')[-1] == 'grounds':
                trajectory = 'gb'
            elif description.split(' into ')[0].split(' ')[-1] == 'bunts':
                trajectory = 'gb'
            elif 'fly ball' in description:
                trajectory = 'fb'
            elif 'ground ball' in description:
                trajectory = 'gb'
            elif 'line drive' in description:
                trajectory = 'ld'
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
            try:
                error = event.split('by')[1].split(' ')[0]
                if error in ['left', 'center', 'right']:
                    trajectory = 'fb'
                else:
                    trajectory = 'gb'
            except IndexError:
                trajectory = 'unknown'
        else:
            if event != 'cs':
                print(event, description)
                raise Exception
            else:
                trajectory = 'none'
    return trajectory
