from flask import render_template, Blueprint

fantasy = Blueprint('fantasy', __name__)


@fantasy.route("/fantasy")
def fantasy_page():
    return render_template('fantasy.html', title="Fantasy")
