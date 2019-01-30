def get_pitcher_on_pickoff_success(pickoff_attempt, xml_file):
    at_bats = []
    with open(xml_file, 'r') as file:
        for at_bat in file.read().split('<at bat'):
            at_bats.append(at_bat)
