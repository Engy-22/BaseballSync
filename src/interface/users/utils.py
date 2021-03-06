import os
from PIL import Image
from flask import url_for, current_app
from flask_mail import Message
from interface import mail


def save_picture(form_picture):
    random_hex = os.urandom(8).hex()
    _, f_ext = os.path.splitext(form_picture.filename)
    picture_fn = random_hex + f_ext
    output_size = (125, 125)
    i = Image.open(form_picture)
    i.thumbnail(output_size)
    i.save(os.path.join(current_app.root_path, 'static/images/profile_pics/', picture_fn))
    return picture_fn


def send_reset_email(user):
    token = user.get_reset_token()
    msg = Message('Password Reset Request', sender='noreply@demo.com', recipients=[user.email])
    msg.body = 'To reset your password visit the following link: {url} \n\nIf you did not make this request then ' \
               'simply ignore this email and no changes will be made.'.format(url=url_for('users.reset_token',
                                                                                          token=token, _external=True))
    mail.send(msg)


def delete_profile_pic(user):
    if user.image_file != 'default.jpg':
        os.remove(os.path.join(current_app.root_path, 'static/images/profile_pics/', user.image_file))
