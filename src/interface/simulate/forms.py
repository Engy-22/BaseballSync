from flask_wtf import FlaskForm
from flask_wtf.file import FileField, FileAllowed
from wtforms import StringField, SubmitField, BooleanField
from wtforms.validators import DataRequired, Length, Email, EqualTo, ValidationError
from flask_login import current_user


class QuickSimForm(FlaskForm):
    team1 = StringField('Away Team', validators=[DataRequired(), Length(min=2, max=20)])
    team2 = StringField('Home Team', validators=[DataRequired(), Length(min=2, max=20)])
    games = StringField('Games', validators=[DataRequired()])
    submit = SubmitField('Simulate')
