from flask import render_template, Blueprint

simulate = Blueprint('simulate', __name__)


@simulate.route("/simulate")
def simulate_page():
    return render_template('simulate.html', title="Simulate")
