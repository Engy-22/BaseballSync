from import_data.player_data.pitch_fx.translators.resolve_player_id import resolve_player_id


def find_pickoff_successes(top_bottom, year, team, xml):
    successes = {}
    with open(xml, 'r') as xml_file:
        half_inning = xml_file.read().replace('\n', '').split(top_bottom + '>')[1]
        if '.  " event="Pickoff ' in half_inning:
            for at_bat in half_inning.split('<atbat'):
                if '.  " event="Pickoff ' in at_bat:
                    pitcher = resolve_player_id(at_bat.split('pitcher="')[1].split('"')[0], year, team, 'pitching')
                    base = at_bat.split('.  " event="Pickoff ')[1][:2]
                    if base not in ['Er', 'At']:
                        if pitcher in successes:
                            successes[pitcher].append(base)
                        else:
                            successes[pitcher] = [base]
        else:
            pass
    return successes
