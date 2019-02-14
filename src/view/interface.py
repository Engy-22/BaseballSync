from flask import Flask, render_template
from forms import RegistrationForm, LoginForm
app = Flask(__name__)
app.config['SECRET_KEY'] = '13e82e0b7361a6e11a886ac181f92c9f'

posts = [{'author': 'me', 'title': 'flask'},
         {'author': 'John Doe', 'title': 'tutorial'}]


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


@app.route("/register")
def register():
    form = RegistrationForm()
    return render_template('register.html', title='Register', form=form)


@app.route("/login")
def login():
    form = LoginForm()
    return render_template('login.html', title='Login', form=form)


if __name__ == '__main__':
    app.run(debug=True)
