from flask_login import login_required
from flask import render_template, Blueprint, url_for, redirect, flash
from interface.simulate.forms import QuickSimForm
from utilities.database.wrappers.baseball_data_connection import DatabaseConnection
from utilities.properties import sandbox_mode
from utilities.get_most_recent_year import get_most_recent_year


simulate = Blueprint('simulate', __name__)


@simulate.route("/simulate")
def simulate_page():
    recent_simulations = ['2/20/2019 CLE vs. DET (50 games)', '2/19/2019 TEX vs. HOU (162 games)']
    return render_template('simulate/simulate.html', title="Simulate", recent_simulations=recent_simulations)


@simulate.route("/simulate/quick_sim", methods=['GET', 'POST'])
@login_required
def quick_sim():
    form = QuickSimForm()
    if form.validate_on_submit():
        games = form.games.data
        return render_template('simulate/sim_results.html', title='Sim Results')
        # return redirect(url_for('simulate.sim_results'))
    league_structure = get_league_structure()
    current_year = str(get_most_recent_year())
    return render_template('simulate/quick_sim2.html', title="Quick Sim", form=form, current_year=current_year,
                           league_structure=league_structure, league_len=len(league_structure[current_year]),
                           division_len=len(league_structure[current_year]['nl']))


@simulate.route("/simulate/sim_results")
@login_required
def sim_results():
    return render_template('simulate/sim_results.html')


def get_league_structure():
    division_names = {'e': 'East', 'c': 'Central', 'w': 'West'}
    league_structure = {}
    db = DatabaseConnection(sandbox_mode)
    years = db.read('select year from years;')
    for year in years:
        league_structure[str(year[0])] = {}
        for league in set(db.read('select league from team_years where year = ' + str(years[0][0]) + ';')):
            league_structure[str(year[0])][league[0]] = {}
            for division in set(db.read('select division from team_years where league = "' + league[0]
                                        + '" and year = ' + str(years[0][0]) + ';')):
                division_name = division_names[division[0]]
                league_structure[str(year[0])][league[0]][division_name] = {}
                for team_id in db.read('select teamId from team_years where division = "' + division[0]
                                       + '" and league="' + league[0] + '" and year = ' + str(years[0][0]) + ';'):
                    league_structure[str(year[0])][league[0]][division_name][team_id[0]] = \
                        db.read('select teamName from teams where teamId="' + team_id[0] + '";')[0][0]
    db.close()
    return league_structure
