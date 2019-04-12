from flask_wtf import FlaskForm
from flask_wtf.file import FileField, FileAllowed
from wtforms import StringField, SubmitField, BooleanField, IntegerField
from wtforms.validators import DataRequired, Length, Email, EqualTo, ValidationError
from flask_login import current_user


class QuickSimForm(FlaskForm):
    games = StringField('Games', validators=[DataRequired()])
    submit = SubmitField('Simulate')
