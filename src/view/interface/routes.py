from flask import render_template, url_for, flash, redirect
from interface import app
from interface.models import User, Post
from interface.forms import RegistrationForm, LoginForm


posts = [
    {
        'author': 'Anthony Raimondo',
        'title': 'Blog Post 1',
        'content': 'First post content',
        'date_posted': 'February 16, 2019'
    },
    {
        'author': 'John Doe',
        'title': 'Blog Post 2',
        'content': 'Second post content',
        'date_posted': 'February 16, 2019'
    }
]


@app.route("/")
@app.route("/home")
def home():
    return render_template('home.html', posts=posts)


@app.route("/about")
def about():
    return render_template('about.html', title="About")


@app.route("/simulate")
def simulate():
    return render_template('simulate.html', title="Simulate")


@app.route("/fantasy")
def fantasy():
    form = LoginForm()
    return render_template('fantasy.html', title='Fantasy', form=form)


@app.route("/register", methods=['GET', 'POST'])
def register():
    form = RegistrationForm()
    if form.validate_on_submit():
        flash(f'Account created for {form.username.data}', 'success')
        return redirect(url_for('home'))
    return render_template('register.html', title='Register', form=form)


@app.route("/login", methods=['GET', 'POST'])
def login():
    form = LoginForm()
    if form.validate_on_submit():
        if form.email.data == 'admin@blog.com' and form.password.data == 'password':
            flash('You have been logged in!', 'success')
            return redirect(url_for('home'))
        else:
            flash('Login Unsuccessful. Please check username and password', 'danger')
    return render_template('login.html', title='Login', form=form)


