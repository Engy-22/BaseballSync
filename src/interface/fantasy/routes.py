from flask import render_template, Blueprint
import random

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
