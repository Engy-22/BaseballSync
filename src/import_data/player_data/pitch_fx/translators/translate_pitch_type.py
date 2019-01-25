def translate_pitch_type(pitch_type):
    pitches = {'FF': '4sm', 'FA': 'fb', 'FT': '2sm', 'FC': 'cut', 'FS': 'split', 'FO': 'pitchout', 'SI': 'sink',
               'SL': 'slide', 'CU': 'curve', 'KC': 'knuckcurve', 'EP': 'eephus', 'CH': 'change', 'SC': 'screw',
               'KN': 'knuck'}
    return pitches[pitch_type]
