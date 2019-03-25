from flask import render_template, request, Blueprint
from interface.models import Post

main = Blueprint('main', __name__)


@main.route("/")
@main.route("/home")
def home():
    page = request.args.get('page', 1, type=int)
    return render_template('home.html', posts=Post.query.order_by(Post.date_posted.desc()).paginate(per_page=5, page=page))


@main.route("/about")
def about():
    return render_template('about.html', title='About')
