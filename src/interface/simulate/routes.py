from flask_login import login_required
from flask import render_template, Blueprint, flash
from interface.simulate.forms import QuickSimForm
import random

simulate = Blueprint('simulate', __name__)


@simulate.route("/simulate")
def simulate_page():
    recent_simulations = ['2/20/2019 CLE vs. DET (50 games)', '2/19/2019 TEX vs. HOU (162 games)']
    return render_template('simulate/simulate.html', title="Simulate", recent_simulations=recent_simulations)


@simulate.route("/simulate/quick_sim", methods=['GET', 'POST'])
@login_required
def quick_sim():
    quick_sim = QuickSimForm()
    recent_simulations = ['2/20/2019 CLE vs. DET (' + str(random.randint(1, 162)) + ' games)', '2/19/2019 TEX vs. HOU ('
                          + str(random.randint(1, 162)) + ' games)']
    return render_template('simulate/quick_sim.html', title="Simulate", recent_simulations=recent_simulations,
                           form=quick_sim)
