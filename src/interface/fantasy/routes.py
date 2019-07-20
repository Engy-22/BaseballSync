import os
import random
import json
from flask import render_template, Blueprint

fantasy = Blueprint('fantasy', __name__)


@fantasy.route("/fantasy")
def fantasy_page():
    fantasy_teams = ['Team Raimondo', 'Sixth City Sluggers']
    return render_template('fantasy/fantasy.html', title="Fantasy", fantasy_teams=fantasy_teams)


@fantasy.route("/fantasy/daily")
def fantasy_daily():
    fantasy_teams = [str(random.randint(1, 5)), str(random.randint(2, 10))]
    return render_template('fantasy/daily.html', title="Daily Fantasy Home", fantasy_teams=fantasy_teams)


@fantasy.route("/fantasy/season")
def fantasy_season():
    fantasy_teams = [str(random.randint(1, 5)), str(random.randint(2, 10))]
    return render_template('fantasy/season.html', title="Season-Long Fantasy", fantasy_teams=fantasy_teams)


@fantasy.route("/fantasy/simulsync")
def simulsync():
    with open(os.path.join('..', 'background', 'strike_zone.json')) as strike_zone_file:
        strike_out_json = json.load(strike_zone_file)
    return render_template('fantasy/simulsync.html', title="SimulSync", strike_zone_dimensions=strike_out_json)
