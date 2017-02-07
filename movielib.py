from flask import Flask
app = Flask(__name__)
from flask import render_template

@app.route('/')
def hello(name=None):
    return render_template('review.html')
