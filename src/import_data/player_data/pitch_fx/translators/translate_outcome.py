def translate_ab_outcome(outcome, description):
    outcomes = {'Single': '1b', 'Double': '2b', 'Triple': '3b', 'Strikeout': 'so', 'Home Run': 'hr', 'Groundout': 'go',
                'Ground Out': 'go', 'Flyout': 'fo', 'Lineout': 'lo', 'Pop Out': 'po', 'Walk': 'bb', 'Sac Bunt': 'sh',
                'Hit By Pitch': 'hbp', 'Intent Walk': 'ibb', 'Forceout': 'fc', 'Line Out': 'lo', 'Field Error': 'error',
                'Grounded Into DP': 'gdp', 'Double Play': 'dp', 'Triple Play': '3p', 'Runner Out': 'cs',
                'Sac Fly': 'sf', 'Bunt Groundout': 'go', 'Fielders Choice': 'fc', 'Fielders Choice Out': 'fc',
                'Sac Fly DP': 'dp', 'Bunt Pop Out': 'po', 'Bunt Lineout': 'po', 'Strikeout - DP': 'so',
                'Catcher Interference': 'error', 'Sacrifice Bunt DP': 'dp', 'Fly Out': 'fo', 'Force Out': 'fc',
                'Bunt Ground Out': 'go'}
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
            elif 'flies out' in description:
                this_outcome = 'fo'
            elif 'grounds out' in description:
                this_outcome = 'go'
            elif 'lines out' in description:
                this_outcome = 'lo'
            elif 'pops out' in description:
                this_outcome = 'po'
            else:
                this_outcome = 'hr'
        elif outcome == 'Batter Interference':
            if 'grounds out' in description:
                this_outcome = 'go'
            elif 'strikes out' in description:
                this_outcome = 'so (swinging)'
            elif 'called out on strikes' in description:
                this_outcome = 'so (looking)'
            elif 'flies out' in description:
                this_outcome = 'fo'
            elif 'lines out' in description:
                this_outcome = 'lo'
            elif 'pops out' in description:
                this_outcome = 'po'
            elif 'double play' in description:
                this_outcome = 'dp'
            else:
                print('\nOutcomes asdfasdf' + outcome)
                this_outcome = "none"
        else:
            print('\nOutcomes ;lkj;lkj' + outcome)
            this_outcome = "none"
    return this_outcome


def translate_pitch_outcome(outcome):
    outcomes = {'Foul': 'foul', 'Foul Tip': 'foul', 'Called Strike': 'strike looking', 'Ball': 'ball',
                'Ball In Dirt': 'ball (in dirt)', 'Swinging Strike': 'strike swinging',
                'Automatic Ball': 'ball (intentional)', 'Swinging Strike (Blocked)': 'strike swinging (in dirt)',
                'Foul (Runner Going)': 'foul', 'Foul Bunt': 'foul (bunt)', 'Missed Bunt': 'strike swinging (bunt)',
                'Pitchout': 'pitchout'}  # make this more specific (i.e. foul/missed bunt, ball in dirt, etc.)
                                         # can then further resolve for pitcher (e.g. missed bunt is important
                                         # for hitter info, but it's just a swinging strike for pitchers.
    try:
        return outcomes[outcome]
    except KeyError:
        print(outcome)
        return 'none'
