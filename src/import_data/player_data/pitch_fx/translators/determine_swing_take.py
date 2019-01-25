def determine_swing_or_take(pitch):
    data = {'Ball In Dirt': 'take', 'Called Strike': 'take', 'Ball': 'take', 'Foul': 'swing', 'Foul Tip': 'swing',
            'In play, no out': 'swing', 'Swinging Strike': 'swing', 'In play, out(s)': 'swing',
            'In play, run(s)': 'swing', 'Foul Bunt': 'swing', 'Hit By Pitch': 'take', 'Intent Ball': 'take',
            'Swinging Strike (Blocked)': 'swing', 'Foul (Runner Going)': 'swing', 'Pitchout': 'take',
            'Missed Bunt': 'swing'}
    return data[pitch]
