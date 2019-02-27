from flask import render_template, Blueprint

fantasy = Blueprint('fantasy', __name__)


@fantasy.route("/fantasy")
def fantasy_page():
    fantasy_teams = ['Team Raimondo', 'Sixth City Sluggers']
    return render_template('fantasy/fantasy.html', title="Fantasy", fantasy_teams=fantasy_teams)
