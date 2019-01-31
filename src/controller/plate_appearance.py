from model.PlateAppearance import PlateAppearance
from controller.pitch import simulate_pitch


def simulate_plate_appearance(batter_id, pitcher_id):
    plate_appearance = PlateAppearance(batter_id, pitcher_id)
    while plate_appearance.get_balls() < 4 and plate_appearance.get_strikes() < 3 and True:  # True = hit, hbp, out, error, etc.
        simulate_pitch(plate_appearance.get_balls(), plate_appearance.get_strikes())
