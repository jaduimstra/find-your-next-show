import pymysql as mdb
import datetime
from flask import render_template, request

from app import app
import graph_analytics as ga

@app.route('/')
@app.route('/index')
def index():
    return render_template("base.html")

@app.route('/output')
def output():
    cities = {"San Francisco":"CA",
              "New York":"NY",
              "Seattle":"WA"}
    city = request.args.get('Venue_city')
    date = request.args.get('Date')
    try:
        datetime.datetime.strptime(date, '%Y-%m-%d')
    except ValueError:
        return render_template("base.html")
    j_graph = ga.generate_json_graph(date, cities[city])
    return render_template("output.html", jsonfile = j_graph, 
                           in_date=date, city=city)
