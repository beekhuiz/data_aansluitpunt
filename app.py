import os
from flask import Flask, jsonify, request, render_template, session, redirect, url_for, flash, send_from_directory
from pymongo import MongoClient
import requests
import gis_functions

app = Flask(__name__)
app.config['SECRET_KEY'] = 'gukfdshkjdsoipee'
my_dir = os.path.dirname(__file__)

app.config['DEVELOP'] = True

@app.route('/')
def index():
    return render_template('index.html')



if __name__ == '__main__':
    if app.config['DEVELOP']:
        app.run(debug=True)                 # DEVELOPMENT
    else:
        app.run(host='0.0.0.0')            # SERVER


