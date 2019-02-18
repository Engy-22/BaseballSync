from flask import render_template, url_for, flash, redirect
from interface import app, db, bcrypt
from interface.models import User, Post
from interface.forms import RegistrationForm, LoginForm
from flask_login import login_user, logout_user


posts = [
    {
        'author': 'Corey Schafer',
        'title': 'Blog Post 1',
        'content': 'First post content',
        'date_posted': 'April 20, 2018'
    },
    {
        'author': 'Jane Doe',
        'title': 'Blog Post 2',
        'content': 'Second post content',
        'date_posted': 'April 21, 2018'
    }
]


@app.route("/")
@app.route("/home")
def home():
    return render_template('home.html', posts=posts)


@app.route("/about")
def about():
    return render_template('about.html', title='About')


@app.route("/register", methods=['GET', 'POST'])
def register():
    form = RegistrationForm()
    if form.validate_on_submit():
        hashed_password = bcrypt.generate_password_hash(form.password.data).decode('utf-8')
        user = User(username=form.username.data, email=form.email.data, password=hashed_password)
        db.session.add(user)
        db.session.commit()
        flash(f'Your account has been created! You may now log in.', 'success')
        return redirect(url_for('login'))
    return render_template('register.html', title='Register', form=form)


@app.route("/login", methods=['GET', 'POST'])
def login():
    form = LoginForm()
    if form.validate_on_submit():
        email = User.query.filter_by(email=form.username_or_email.data).first()
        username = User.query.filter_by(username=form.username_or_email.data).first()
        if email and bcrypt.check_password_hash(email.password, form.password.data):
            flash('Welcome, ' + email.username, 'success')
            login_user(email, remember=form.remember.data)
            return redirect(url_for('home'))
        elif username and bcrypt.check_password_hash(username.password, form.password.data):
            login_user(username, remember=form.remember.data)
            flash('Welcome, ' + username.username, 'success')
            return redirect(url_for('home'))
        else:
            flash('Login Unsuccessful. Please check username and password', 'danger')
    return render_template('login.html', title='Login', form=form)


@app.route("/logout")
def logout():
    logout_user()
    return redirect(url_for('home'))


@app.route("/account")
def account():
    return render_template('account.html', title='Account')
