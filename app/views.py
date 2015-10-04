from flask import render_template, request
import pymysql as mdb
from app import app
import graph_analytics as ga

@app.route('/')
@app.route('/index')
def index():
    return render_template("base.html")

@app.route('/output')
def output():
    city = request.args.get('Venue_city')
    date = request.args.get('Date')
    #j_graph  = 'static/js/json/sf_sept15.json'
    #j_graph  = 'static/js/json/foo.json'
    #j_graph = 'static/js/json/2015-10-07_NY.json'
    j_graph = ga.generate_json_graph(date, city)
    return render_template("output.html", jsonfile = j_graph)

