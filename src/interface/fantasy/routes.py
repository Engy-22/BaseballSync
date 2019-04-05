from flask import render_template, Blueprint
import random

fantasy = Blueprint('fantasy', __name__)


@fantasy.route("/fantasy")
def fantasy_page():
    fantasy_teams = ['Team Raimondo', 'Sixth City Sluggers']
    return render_template('fantasy/fantasy.html', title="Fantasy", fantasy_teams=fantasy_teams)


@fantasy.route("/fantasy/daily")
def fantasy_daily():
    recent_simulations = [str(random.randint(1, 5)), str(random.randint(2, 10))]
    return render_template('fantasy/daily.html', title="Daily Fantasy Home", recent_simulations=recent_simulations)


@fantasy.route("/fantasy/season")
def fantasy_season():
    recent_simulations = [str(random.randint(1, 5)), str(random.randint(2, 10))]
    return render_template('fantasy/season.html', title="Season-Long Fantasy", recent_simulations=recent_simulations)
