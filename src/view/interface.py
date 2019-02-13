from flask import Flask, render_template, url_for
app = Flask(__name__)

posts = [{'author': 'me', 'title': 'flask'},
         {'author': 'John Doe', 'title': 'tutorial'}]


@app.route("/")
@app.route("/home")
def home():
    return render_template('home.html', posts=posts)


@app.route("/about")
def about():
    return render_template('about.html', title="About")


if __name__ == '__main__':
    app.run(debug=True)